"""Tests for how main turns a job request message into a runner image, special PVs and taints."""

import importlib
import random
from unittest import mock

import pytest

DEFAULT_SHA = "deadbeef" * 8
IMAGING_SHA = "cafebabe" * 8
WATCHER_SHA = "abcdef12" * 8

DEFAULT_RUNNER = f"ghcr.io/fiaisis/mantid@sha256:{DEFAULT_SHA}"
IMAGING_RUNNER = f"ghcr.io/fiaisis/mantidimaging@sha256:{IMAGING_SHA}"


@pytest.fixture
def main_module(monkeypatch):
    """The main module reads its runner images at import time, so it is reloaded per test."""
    monkeypatch.setenv("DEFAULT_RUNNER_SHA", DEFAULT_SHA)
    monkeypatch.setenv("IMAGING_RUNNER_SHA", IMAGING_SHA)
    monkeypatch.setenv("WATCHER_SHA", WATCHER_SHA)
    with mock.patch("jobcreator.job_creator.load_kubernetes_config"):
        # Imported here rather than at the top of the file as main reads its runner images from the
        # environment at import time, and raises if they are missing.
        job_creator_main = importlib.import_module("jobcreator.main")

        yield importlib.reload(job_creator_main)


def _autoreduction_message(instrument, additional_values):
    return {
        "job_type": "autoreduction",
        "filepath": f"/archive/NDX{instrument}/Instrument/data/cycle_26_1/{instrument}00012345.nxs",
        "experiment_number": "123456",
        "instrument": instrument,
        "experiment_title": "A test experiment",
        "users": "Nobody",
        "run_start": "2026-01-01T09:00:00",
        "run_end": "2026-01-01T09:05:00",
        "raw_frames": "100",
        "good_frames": "90",
        "additional_values": additional_values,
    }


def _ngem_values():
    return {"recon": "false", "ngem": "true", "ngem_path": "/ngem/nGEM-INES/DATA/IMAT_2026_01/IMAT00012345"}


def _recon_values():
    return {"recon": "true", "ngem": "false", "images_dir": "/imat/RB123456/Tomo", "runno": 12345}


def test_imat_recon_uses_imaging_runner(main_module):
    assert main_module._select_runner_image("IMAT", is_ngem=False) == IMAGING_RUNNER


def test_imat_ngem_uses_default_runner(main_module):
    assert main_module._select_runner_image("IMAT", is_ngem=True) == DEFAULT_RUNNER


def test_ngem_on_another_instrument_uses_default_runner(main_module):
    assert main_module._select_runner_image("IRIS", is_ngem=True) == DEFAULT_RUNNER


def test_other_instruments_use_default_runner(main_module):
    assert main_module._select_runner_image("MARI", is_ngem=False) == DEFAULT_RUNNER


def test_imat_falls_back_to_default_runner_without_imaging_sha(main_module, monkeypatch):
    monkeypatch.delenv("IMAGING_RUNNER_SHA")
    module = importlib.reload(main_module)
    assert module._select_runner_image("IMAT", is_ngem=False) == DEFAULT_RUNNER


@pytest.mark.parametrize(
    ("instrument", "is_ngem", "expected"),
    [
        ("IMAT", False, ["imat"]),
        ("IMAT", True, ["ngem"]),
        ("IRIS", True, ["ngem"]),
        ("GEM", False, ["gem"]),
        ("MARI", False, []),
        ("MARI", True, ["ngem"]),
    ],
)
def test_generate_special_pvs(main_module, instrument, is_ngem, expected):
    assert main_module._generate_special_pvs(instrument, is_ngem) == expected


def test_imat_recon_gets_gpu_taints(main_module):
    taints, affinity = main_module._select_taints_and_affinity("IMAT", is_ngem=False)
    assert taints == [{"key": "nvidia.com/gpu", "effect": "NoSchedule", "operator": "Exists"}]
    assert affinity == {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}


@pytest.mark.parametrize("instrument", ["IMAT", "IRIS"])
def test_ngem_runs_get_no_taints_or_affinity(main_module, instrument):
    assert main_module._select_taints_and_affinity(instrument, is_ngem=True) == ([], None)


@pytest.mark.parametrize(
    ("additional_values", "expected_image", "expected_pvs", "expected_gpu_taints"),
    [
        (_ngem_values(), DEFAULT_RUNNER, ["ngem"], False),
        (_recon_values(), IMAGING_RUNNER, ["imat"], True),
    ],
)
def test_autoreduction_message_spawned_with_matching_runner_and_pvs(
    main_module, additional_values, expected_image, expected_pvs, expected_gpu_taints
):
    job_id = random.randint(1, 100)  # noqa: S311
    with (
        mock.patch.object(main_module.JOB_CREATOR, "spawn_job") as spawn_job,
        mock.patch("jobcreator.main.post_autoreduction_job", return_value=("print('reduced')", job_id)),
        mock.patch("jobcreator.main.create_ceph_mount_path_autoreduction", return_value="/ceph/IMAT/RB123456"),
    ):
        main_module.process_autoreduction_message(_autoreduction_message("IMAT", additional_values))

    assert spawn_job.call_count == 1
    kwargs = spawn_job.call_args.kwargs
    assert kwargs["runner_image"] == expected_image
    assert kwargs["special_pvs"] == expected_pvs
    assert bool(kwargs["taints"]) is expected_gpu_taints
    assert kwargs["job_id"] == job_id
