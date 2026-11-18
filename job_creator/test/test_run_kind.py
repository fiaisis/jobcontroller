"""Tests for run kind detection, which decides when a message describes an nGEM detector run."""

import pytest

from jobcreator.run_kind import is_ngem_run


@pytest.mark.parametrize(
    "message",
    [
        {"additional_values": {"ngem": "true"}},
        {"additional_values": {"ngem": "True"}},
        {"additional_values": {"ngem": True}},
        {"additional_values": '{"ngem": "true"}'},
        {"additional_values": {"recon": "false", "ngem": "true", "ngem_path": "/ngem/somewhere"}},
        {"ngem": "true"},
    ],
)
def test_is_ngem_run_true(message):
    assert is_ngem_run(message) is True


@pytest.mark.parametrize(
    "message",
    [
        {},
        {"additional_values": {}},
        {"additional_values": {"ngem": "false", "recon": "true", "images_dir": "/imat/Tomo"}},
        {"additional_values": {"runno": 100}},
        {"additional_values": {"ngem": "false"}, "ngem": "true"},
        {"additional_values": "not json at all"},
        {"additional_values": None},
        {"additional_values": ["not", "a", "dict"]},
        {"ngem": "false"},
    ],
)
def test_is_ngem_run_false(message):
    assert is_ngem_run(message) is False
