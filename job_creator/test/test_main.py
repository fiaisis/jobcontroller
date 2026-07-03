import importlib
import sys
from unittest import mock

import pytest


def import_main_module(monkeypatch):
    monkeypatch.setenv("DEFAULT_RUNNER_SHA", "default-runner-sha")
    monkeypatch.setenv("WATCHER_SHA", "watcher-sha")
    with mock.patch("jobcreator.job_creator.JobCreator"):
        if "jobcreator.main" in sys.modules:
            return importlib.reload(sys.modules["jobcreator.main"])
        return importlib.import_module("jobcreator.main")


def test_process_message_unknown_job_type_raises(monkeypatch):
    main_module = import_main_module(monkeypatch)

    with pytest.raises(ValueError, match="message type not recognised"):
        main_module.process_message({"job_type": "unexpected"})


def test_main_passes_failure_queue_name_to_consumer(monkeypatch):
    monkeypatch.setenv("INGRESS_QUEUE_NAME", "scheduled-jobs")
    monkeypatch.setenv("FAILURE_QUEUE_NAME", "custom-failed-jobs")
    main_module = import_main_module(monkeypatch)

    with mock.patch.object(main_module, "QueueConsumer") as queue_consumer:
        main_module.main()

    queue_consumer.assert_called_once_with(
        main_module.process_message,
        queue_host="",
        username="",
        password="",
        queue_name="scheduled-jobs",
        failure_queue_name="custom-failed-jobs",
    )
    queue_consumer.return_value.start_consuming.assert_called_once_with(main_module.write_readiness_probe_file)
