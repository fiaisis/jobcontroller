# Job Controller

![License: GPL-3.0](https://img.shields.io/github/license/fiaisis/jobcontroller)
![Build: passing](https://img.shields.io/github/actions/workflow/status/fiaisis/jobcontroller/tests.yml?branch=main)
[![codecov](https://codecov.io/github/fiaisis/jobcontroller/branch/main/graph/badge.svg?token=XR6PCJ1VR8)](https://codecov.io/github/fiaisis/jobcontroller)
[![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-30173d)](https://docs.astral.sh/ruff/)
[![linting: ruff](https://img.shields.io/badge/linting-ruff-30173d)](https://docs.astral.sh/ruff/)

The software responsible for controlling the creation of Jobs, and notifying the rest of the software about job completion. It is split into 2 parts, the Job Creator, and the Job Watcher. Their names are self explanatory, the Job Creator is responsible for receiving messages and creating the workloads on a kubernetes cluster it is present within or pointed at via a Kubeconfig file, and the Job Watcher is a side car container charged with watching the job for progression, as long as the job doesn't seemingly freeze, on the finishing of the job's main container, the watcher will update the database via the API.

The relevant README.md files are available for both the Job Watcher and the Job Creator in their respective subdirectories of this repository.

# How to Container

## Job creator
Build container image
```bash
docker build . -f container/job_creator.Dockerfile -t ghcr.io/fiaisis/jobcreator
```
Push container image
```bash
docker push ghcr.io/fiaisis/jobcreator
```

## Job watcher
Build container image
```bash
docker build . -f container/job_watcher.Dockerfile -t ghcr.io/fiaisis/jobwatcher
```
Push container image
```bash
docker push ghcr.io/fiaisis/jobwatcher
```
