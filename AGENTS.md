 # AGENTS.md

Guidance for coding agents working in this repository.

## What this repo is

The `jobcontroller` repository contains two independently packaged Python components that manage
the execution lifecycle of reduction and analysis jobs on Kubernetes in the Flexible Interactive
Automation (FIA) system (see root `README.md` and `job_creator/README.md`):

1. **`jobcreator` (`job_creator/`)**: Consumes job request messages from a RabbitMQ quorum queue
   (`INGRESS_QUEUE_NAME`), handles three message types (`autoreduction`, `rerun`, and `simple`),
   acquires the transformed reduction script and database `job_id` from `FIA-API`
   (`POST /job/autoreduction`) for autoreduction runs, provisions per-job Kubernetes `PersistentVolume`s
   and `PersistentVolumeClaim`s (`/archive`, `/output` on CephFS, `/extras` on Manila, plus
   instrument-specific mounts such as `/imat` + 128Gi `/dev/shm` + GPU scheduling or `/gem`), and
   spawns a `batch/v1` Kubernetes `Job` containing the runner main container and a `job-watcher`
   sidecar container.
2. **`jobwatcher` (`job_watcher/`)**: Runs as the sidecar container inside each spawned Kubernetes
   `Job` pod. It polls the main runner container's status and logs via the Kubernetes API, detects
   completion, errors, or stalls (no new log output for 30 minutes, or total runtime exceeding
   `MAX_TIME_TO_COMPLETE_JOB`, default 6 hours), parses the final JSON status line printed by the
   runner script (or extracts the latest Python traceback on failure), updates the job record in
   `FIA-API` (`PATCH /job/{job_id}`), and deletes the per-job PVs and PVCs before exiting.

## Position in the FIA service

### Overarching FIA architecture

FIA is a multi-repository system (all checked out as siblings under the same parent workspace
directory) that automatically reduces instrument data, and also lets users browse, plot, and rerun
that data interactively:

```text
[ISIS Instrument Archive (/archive, /imat) & lastrun.txt]
        │
        ▼
  file-watcher (Kopf Operator + Per-Instrument Watcher Pods)
        │  publishes file path to RabbitMQ: `watched-files`
        ▼
  run-detection ◄──── fetches instrument rules (`GET /instrument/{inst}/specification`) ────► FIA-API
        │  ingests .nxs metadata, verifies rules, publishes JobRequest JSON
        │  to RabbitMQ: `scheduled-jobs` (or retries via `failed-watched-files`)
        ▼
  jobcontroller (job_creator — THIS REPO) ◄── posts `POST /job/autoreduction` ──► FIA-API ◄──► autoreduction-scripts
        │  spawns Kubernetes Job in cluster:
        ├──► Main container: runners (`ghcr.io/fiaisis/mantid` or `mantid-imaging`) -> writes reduced data to `/ceph`
        └──► Sidecar container: jobcontroller (job_watcher — THIS REPO) -> updates job in FIA-API (`PATCH /job/{id}`)

[Interactive / Web & Live Data Paths]
  scigateway (Host App) ──► frontend (FIA React Plugin)
                                ├──► fia-auth (UOWS login, JWT verify/refresh, `/experiments` RB lookup)
                                ├──► FIA-API (runs, jobs, scripts, specifications, reruns -> `watched-files` / `scheduled-jobs`)
                                └──► plotting-service (h5grove NeXus/HDF5 & IMAT image serving from `/ceph` & `/imat`)

  Kafka (`<INST>_events`, `<INST>_sampleEnv`, `<INST>_runInfo`)
        └──► live-data (Live Data Operator + Mantid Live Data Processor fetching `/live-data/{inst}/script` from FIA-API)

  LLSP (Long Lived Script Processors: `LLSP-API` + RabbitMQ + Celery `LLSP-Worker` built on `ghcr.io/fiaisis/mantid`)

[Infrastructure & Deployment]
  k8s (Terraform + Ansible for OpenStack clusters, Redpanda, local Minikube)
  gitops (ArgoCD App-of-Apps & Kustomize `base`/`envs`/`variants` manifests for all FIA services)
```

### This repository's location (upstream, downstream & siblings)

- **Role**: Bridges RabbitMQ job scheduling and Kubernetes execution, orchestrating `runners`
  containers, storage mounts, and status reporting back to `FIA-API`.
- **Upstream inputs (what triggers `jobcreator`)**:
  - RabbitMQ queue (`INGRESS_QUEUE_NAME`, typically `scheduled-jobs`) carrying three JSON payload
    flavors (see `job_creator/README.md` for full examples):
    - **`autoreduction`** (default when `job_type` is omitted): published by `run-detection`
      (`../run-detection`).
    - **`rerun`**: published by `FIA-API` (`../FIA-API`) when a user triggers a rerun with a
      pre-built `script`, `job_id`, and `runner_image`.
    - **`simple`**: published by `FIA-API` for arbitrary/simple runner jobs owned by a
      `user_number` or `experiment_number`.
- **Downstream outputs (what `jobcreator` and `jobwatcher` call or provision)**:
  - `FIA-API` (`../FIA-API`):
    - `jobcreator` calls `POST /job/autoreduction` (with `Authorization: Bearer <FIA_API_API_KEY>`)
      to create the job record and acquire `(script, job_id)`.
    - `jobwatcher` calls `PATCH http://{FIA_API_HOST}/job/{job_id}` with `state` (`SUCCESSFUL`,
      `UNSUCCESSFUL`, `ERROR`), `status_message`, `outputs`, `start`, `stacktrace`, and `end`.
  - Kubernetes API (`batch/v1` Jobs, `v1` PVs/PVCs/Pods) — spawns runner pods using images from
    `runners` (`../runners`, pinned via `DEFAULT_RUNNER_SHA` and `IMAGING_RUNNER_SHA`) that write
    reduced files to `/output` (mounted from CephFS `/ceph`, later served by `../plotting-service`).
  - `gitops` (`../gitops`) — `.github/workflows/build_and_push.yml` builds and pushes both
    `ghcr.io/fiaisis/jobcreator` and `ghcr.io/fiaisis/jobwatcher` on every push to `main`, and
    automatically updates the `jobcreator` image digest and `WATCHER_SHA` env var in
    `./components/jobcreator/envs/staging/jobcreator.yml`.
- **Sibling repositories to check when changing contracts**: `../run-detection` (`JobRequest` JSON
  fields), `../FIA-API` (`POST /job/autoreduction`, `PATCH /job/{id}`, and rerun/simple queue
  payloads), `../runners` (runner entrypoint `python -c "$@"` and final-line JSON stdout contract
  `{"status": ..., "status_message": ..., "output_files": ..., "stacktrace": ...}`), and
  `../gitops` (`components/jobcreator/` manifests and env vars).

### Core technologies
- **Python**: version/minimum is defined by `requires-python` in `job_creator/pyproject.toml` and
  `job_watcher/pyproject.toml` — do not hardcode a version here; check those files and
  `.github/workflows/` before creating an environment.
- **kubernetes**: Official Kubernetes Python client (`CoreV1Api`, `BatchV1Api`).
- **pika**: RabbitMQ blocking client used by `jobcreator` to consume job messages.
- **requests**: HTTP client used by `jobcreator` and `jobwatcher` to talk to `FIA-API` and GHCR.
- **pytest** (+ `pytest-cov`, `pytest-random-order`, and `hypothesis` in `job_watcher`): Unit testing.
- **ruff**: Linting and formatting (`line-length = 120`, rules in both `pyproject.toml` files).
- **mypy**: Static type checking (`mypy --strict job_creator/jobcreator` and
  `mypy --strict job_watcher/jobwatcher`).
- **Docker**: Two Dockerfiles (`container/job_creator.Dockerfile` and
  `container/job_watcher.Dockerfile`).

---

## Repository structure

```
job_creator/                 JobCreator subproject (independent Python package `jobcreator`)
  jobcreator/                Application source: CLI entrypoint (`jobcreator`), RabbitMQ consumer,
                              FIA-API script acquisition (`POST /job/autoreduction`), K8s PV/PVC
                              builders (SMB /archive & /imat, Manila /extras & /gem, CephFS /output),
                              and BatchV1 Job spawning with taints/affinities/GPU runtime
  test/                      Pytest unit test suite for `jobcreator` plus fixture files
  pyproject.toml, README.md  Package metadata, pinned dependencies, ruff config, and JSON message
                              payload examples (autoreduction, rerun, simple)

job_watcher/                 JobWatcher subproject (independent Python package `jobwatcher`)
  jobwatcher/                Application source: CLI entrypoint (`jobwatcher`), pod status/stall
                              monitoring, raw UTF-8 pod log reader & last-line JSON parser, stacktrace
                              extractor, `PATCH /job/{id}` updater, and PV/PVC cleanup
  test/                      Pytest unit test suite for `jobwatcher`
  pyproject.toml, README.md  Package metadata, pinned dependencies, and ruff config

container/                   Dockerfiles for `jobcreator` (`job_creator.Dockerfile`) and
                              `jobwatcher` (`job_watcher.Dockerfile`)
.github/workflows/           CI/CD workflows: tests.yml, formatting_and_linting.yml,
                              build_and_push.yml, trivy.yml
```

---

## Dual `pyproject.toml` files — read these first

This repository has **no root `pyproject.toml`**; instead, `job_creator/pyproject.toml` and
`job_watcher/pyproject.toml` are the sources of truth for their respective packages:

- Runtime dependencies and optional extras (`formatting`, `test`, `all`) are pinned with `==` in
  both files — keep new dependencies pinned the same way, and if you upgrade a shared tool (`ruff`,
  `mypy`, `pytest`, `kubernetes`), keep both `pyproject.toml` files aligned.
- `requires-python` in each `pyproject.toml` defines the minimum supported Python version; check it
  and `.github/workflows/` before creating a local environment.
- CI runs `mypy --strict job_creator/jobcreator` and `mypy --strict job_watcher/jobwatcher` from the
  repository root.

---

## Configuration & runtime conventions

### `jobcreator` environment variables
| Variable | Default / Requirement | Description |
| :--- | :--- | :--- |
| `DEFAULT_RUNNER_SHA` | *(required — raises `OSError` if unset)* | `sha256` digest for `ghcr.io/fiaisis/mantid@sha256:...` |
| `IMAGING_RUNNER_SHA` | `None` (falls back to `DEFAULT_RUNNER` if unset) | `sha256` digest for `ghcr.io/fiaisis/mantidimaging@sha256:...` (used for IMAT) |
| `WATCHER_SHA` | *(required — raises `OSError` if unset)* | `sha256` digest for `ghcr.io/fiaisis/jobwatcher@sha256:...` sidecar |
| `DEV_MODE` | `"False"` | When truthy (not `"false"`), mounts a 100Gi `emptyDir` at `/output` instead of CephFS |
| `FIA_API` | `fia-api-service.fia.svc.cluster.local:80` | Host:port for `FIA-API` (`POST /job/autoreduction`, passed to watcher as `FIA_API_HOST`) |
| `FIA_API_API_KEY` | `""` | Bearer token for `FIA-API` requests |
| `QUEUE_HOST` | `""` | RabbitMQ hostname |
| `INGRESS_QUEUE_NAME` | `""` | RabbitMQ queue consumed by `jobcreator` |
| `QUEUE_USER` / `QUEUE_PASSWORD` | `""` | RabbitMQ credentials |
| `JOB_NAMESPACE` | `fia` | Kubernetes namespace where Jobs, PVs, and PVCs are created |
| `CEPH_CREDS_SECRET_NAME` / `CEPH_CREDS_SECRET_NAMESPACE` | `ceph-creds` / `fia` | Kubernetes Secret for CephFS mounts |
| `CLUSTER_ID` / `FS_NAME` | `ba68226a-672f-4ba5-97bc-22840318b2ec` / `deneb` | Ceph cluster ID and filesystem name |
| `MANILA_SHARE_ID` / `MANILA_SHARE_ACCESS_ID` | *(defaults in `main.py`)* | OpenStack Manila share IDs for `/extras` and `/gem` |
| `MAX_TIME_TO_COMPLETE` | `21600` (6 hours) | Maximum job runtime in seconds passed to `jobwatcher` |

### `jobwatcher` environment variables
| Variable | Default | Description |
| :--- | :--- | :--- |
| `FIA_API` | `fia-api-service.fia.svc.cluster.local:80` | Host:port for `PATCH http://{FIA_API}/job/{job_id}` |
| `FIA_API_API_KEY` | `None` | Bearer token for `FIA-API` requests |
| `JOB_NAMESPACE` | `fia` | Kubernetes namespace of the watched Job/Pod |
| `JOB_NAME` / `POD_NAME` / `CONTAINER_NAME` | `""` | Target Job name, partial Pod name, and main runner container name |
| `MAX_TIME_TO_COMPLETE_JOB` | `21600` (6 hours) | Timeout in seconds before a running pod is treated as stalled |

### Pod log parsing & instrument special cases
- **Raw pod log reading (`_read_pod_logs`)**: `jobwatcher` calls
  `v1_core.read_namespaced_pod_log(_preload_content=False, ...)` and decodes UTF-8 bytes directly so
  `kubernetes-client`'s `ApiClient.deserialize()` does not corrupt JSON log lines into single-quoted
  Python `dict` reprs or `b'...'` byte-repr strings. Preserve this pattern when touching log
  handling.
- **IMAT & GEM special handling in `jobcreator`**:
  - `imat`: Selects `IMAGING_RUNNER`, mounts `/imat` (SMB) and a 128Gi memory-backed `/dev/shm`
    `emptyDir`, sets `runtime_class_name="nvidia"`, requests `nvidia.com/gpu: "1"`, and adds GPU
    toleration + `gpu-worker` node affinity.
  - `gem`: Mounts a read-write Manila share at `/gem`.

---

## Environment & running commands

Do not assume any particular machine already has a conda env, kubectl context, or virtualenv set up
— check what is available first. Prefer `conda` when available (`command -v conda` on Linux/macOS or
`where conda` on Windows); fall back to a Python `venv` only if conda is not installed.

1. Check existing conda environments:
   ```shell
   conda env list
   ```
   If a matching env exists (e.g. `jobcontroller`), use it with `conda run -n <env-name> <command>`
   or `conda activate <env-name>`.
2. If no suitable conda env exists but `conda` is installed, create one using the Python version
   from `pyproject.toml` / `.github/workflows/` and install both subpackages:
   ```shell
   conda create -n jobcontroller python=<version-from-pyproject.toml-or-CI> -y
   conda run -n jobcontroller pip install "./job_creator[all]" "./job_watcher[all]"
   ```
3. Only if `conda` is not installed, fall back to a virtualenv:
   ```shell
   python3 -m venv .venv        # Linux/macOS (use `python` on Windows)
   . .venv/bin/activate         # POSIX shell (.venv\Scripts\Activate.ps1 on PowerShell)
   pip install "./job_creator[all]" "./job_watcher[all]"
   ```

### Running tests, linting & containers
- **Unit tests, linting & type checking (from repo root, matching CI)**:
  ```shell
  conda run -n <env-name> pytest ./job_creator --random-order --random-order-bucket=global
  conda run -n <env-name> pytest ./job_watcher --random-order --random-order-bucket=global
  conda run -n <env-name> ruff check .
  conda run -n <env-name> ruff format --check .
  conda run -n <env-name> mypy --strict job_creator/jobcreator
  conda run -n <env-name> mypy --strict job_watcher/jobwatcher
  ```
- **Docker builds** (from repo root, per `README.md`):
  ```shell
  docker build . -f container/job_creator.Dockerfile -t ghcr.io/fiaisis/jobcreator
  docker build . -f container/job_watcher.Dockerfile -t ghcr.io/fiaisis/jobwatcher
  ```

## Kubernetes commands

Available kubectl contexts vary per machine/operator — always run `kubectl config get-contexts`
first before targeting a cluster. Never rely on the current context; always pass `--context`
explicitly at the **end** of the command:

```shell
kubectl get pods -n fia --context <context-name>
kubectl get jobs -n fia --context <context-name>
kubectl logs deploy/jobcreator -n fia --context <context-name>
```

Double- and triple-check the context name before running anything against a production-like cluster.

## Managing issues

This repo has no local `.github/ISSUE_TEMPLATE/`; GitHub serves the org-wide default template from
`fiaisis/.github` instead. Always use it rather than inventing an ad hoc issue format:

```shell
gh issue create --template Issue
```

Check `gh issue list` / `gh issue view <number>` first to avoid duplicates.

## Branch naming

Branches use `snake_case` descriptions of the work being done. Do not use category prefixes like
`feature/` or `fix/`.

- **Format**: `description_of_work` (e.g. `gem_special_pv`, `enlarge_imat_memory`).

## Commit messages

`main`'s history (`git log --oneline main`) is predominantly **squash merges**: one commit per PR,
titled `<PR title> (#<PR number>)`, e.g. `Fix jobwatcher failing to parse JSON job output from pod
logs (#466)`. This has two practical consequences:

- **The PR title is what actually survives into permanent history.** Individual commits on a working
  branch are squashed on merge, so getting the PR title right (see "Pull request process" below)
  matters more than crafting a commit-by-commit history.
- **Individual commit messages still matter for review and for the squash commit body.** By default,
  GitHub's squash merge concatenates every commit subject on the branch into the squashed commit's
  body. Write each commit as a focused, reviewable unit of work with a clear, imperative-mood
  summary line (and a short *why* body paragraph on non-trivial commits) — avoid noise like `wip` or
  `fix` that would leak into the squashed body. Leave auto-generated Dependabot commit messages as-is.

## Requirements for code changes

Only run tests, linters, and type checkers when they are **relevant to the files you actually
changed** — if a change only touches documentation (e.g. `.md` files), skip `pytest`, `ruff`, and
`mypy`. When you do change Python code, dependency manifests, or configuration, before considering
the change complete:
1. **Add or update unit tests** in `job_creator/test/` or `job_watcher/test/` (whichever subproject
   you changed) for any new or changed behavior.
   *(Note: `.github/workflows/tests.yml` passes `--ignore=test/database/test_db_updater_integration.py`
   to both `pytest` invocations, but no `test/database/` directory currently exists in either
   subproject — this appears to be stale/inert CI config rather than a real integration suite to
   extend; don't assume a DB-backed integration test suite exists unless you find one.)*
2. **Run the linter and type checker** for both subprojects (`ruff check .`, `ruff format .`,
   `mypy --strict job_creator/jobcreator`, `mypy --strict job_watcher/jobwatcher`) and fix
   everything reported.
3. **Run both unit test suites with coverage** and confirm coverage has not regressed compared to
   `main` (CI uploads results to Codecov separately for `jobcreator` and `jobwatcher`; locally, add
   `--cov-report=term-missing` to the `pytest` commands in "Environment & running commands").

## Pull request process

1. **Local validation** before opening a PR (when `.py` / `pyproject.toml` / container files are
   changed; skip for `.md`-only changes):
   ```shell
   conda run -n <env-name> pytest ./job_creator --random-order --random-order-bucket=global
   conda run -n <env-name> pytest ./job_watcher --random-order --random-order-bucket=global
   conda run -n <env-name> ruff check .
   conda run -n <env-name> ruff format .
   conda run -n <env-name> mypy --strict job_creator/jobcreator
   conda run -n <env-name> mypy --strict job_watcher/jobwatcher
   ```
   Note: the CI workflow (`.github/workflows/formatting_and_linting.yml`) also runs `ruff format .`,
   `ruff check --fix`, and `mypy --strict` on both packages on push and auto-commits any fixes back
   to the branch as `"Formatting and linting commit"`.
2. **PR title**: a clear, concise description in normal grammar/sentence case (e.g. `Enlarge IMAT
   memory`, `Fix jobwatcher failing to parse JSON job output from pod logs`). Do not use
   `snake_case` for PR titles.
3. **PR description**: use the org-wide default template from `fiaisis/.github`
   (`.github/pull_request_template.md` — no local override in this repo), which is just
   `Closes # <Issue Number>` followed by a `## Description` section. Ask the user for the issue
   number if one isn't evident from the task, or confirm there is none to link.
4. **AI-assistance label**: attach a label to the PR indicating that it was made with the assistance
   of AI. Check existing labels first with `gh label list` (e.g. `AI` or `AI_code` in this org) and
   apply the matching label via `gh pr create --label <label>` or
   `gh pr edit <number> --add-label <label>`; if the repo does not have an AI label yet, create one
   first (`gh label create "AI" --description "Created with the assistance of AI"`).

## Handling Dependabot PRs

Dependabot creates daily PRs grouped as `jobcreator-python-packages` (`/job_creator`),
`jobwatcher-python-packages` (`/job_watcher`), `action-packages` (`/`), and `dockerfiles`
(`/container`) per `.github/dependabot.yml`. When asked to handle these:

1. **List and identify**: `gh pr list` to find open Dependabot PRs.
2. **Review changes**: `gh pr diff <number>`, paying close attention to major version bumps in
   `job_creator/pyproject.toml`, `job_watcher/pyproject.toml`, `container/*.Dockerfile`, or
   `.github/workflows/`.
3. **Validate**:
   - Python/dependency changes: `gh pr checkout <number>`, install updated dependencies in the
     affected package (`pip install "./job_creator[all]" "./job_watcher[all]"`), and run both test
     suites, `ruff`, and `mypy --strict` as in the PR process above.
   - Docker/Action changes: verify CI checks pass with `gh pr checks <number>`.
4. **Approve and merge** once validation passes:
   ```shell
   gh pr review <number> --approve
   gh pr merge <number> --merge
   ```
   Use `--admin` if blocked by branch protection (`gh pr merge <number> --merge --admin`).
5. **Clean up**: close redundant/superseded PRs with `gh pr close <number> --delete-branch`.
6. **Final verification**: checkout `main`, pull latest changes, and re-run both `pytest` suites to
   confirm `main` is green.

## General working practices

- Prefer the `read` tool (with `offset`/`limit`) over `sed -n`, `cat`, or similar shell one-liners
  when inspecting file contents, and prefer the `write` and `edit` tools over `bash` (`cat <<EOF`,
  `sed -i`, `echo >`), Python one-liners, or other non-tool-based commands whenever creating or
  modifying files.
- Ask the user a clarifying question whenever a task is ambiguous (e.g. whether a change applies to
  `jobcreator`, `jobwatcher`, or both, which message type or instrument PV is affected, or whether
  an issue number exists to link in a PR) rather than guessing.
- Keep dependency versions pinned with `==` in both `pyproject.toml` files consistent with the
  existing style.

## Keeping this document up to date

If a change you make would make any information in this `AGENTS.md` inaccurate or incomplete —
e.g. a new environment variable, a new job message type or instrument special PV, a changed
directory layout, a new/renamed CI workflow, an updated Dependabot grouping, a new sibling-repo
contract, or a different testing/linting command — update the relevant section of this file as part
of the same change. Treat it the same way you would treat `README.md`: documentation that goes stale
is worse than no documentation, so don't leave it to a future agent or PR to notice the drift.
