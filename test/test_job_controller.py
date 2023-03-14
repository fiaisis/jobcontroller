# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import os
import unittest
from pathlib import Path
from unittest import mock

from job_controller.job_controller import JobController


class JobControllerTest(unittest.TestCase):
    @mock.patch("job_controller.job_controller.JobCreator")
    @mock.patch("job_controller.job_controller.TopicConsumer")
    def setUp(self, _, __):
        self.joc = JobController()

    @mock.patch("job_controller.job_controller.JobCreator")
    @mock.patch("job_controller.job_controller.TopicConsumer")
    def test_job_controller_gets_kafka_ip_from_env(self, _, __):
        self.assertEqual(self.joc.kafka_ip, None)

        os.environ["KAFKA_IP"] = "random_ip_address_from_kafka"

        self.assertEqual(JobController().kafka_ip, "random_ip_address_from_kafka")

        os.environ.pop("KAFKA_IP")

    @mock.patch("job_controller.job_controller.JobWatcher")
    @mock.patch("job_controller.job_controller.acquire_script")
    @mock.patch("job_controller.job_controller.create_ceph_path")
    def test_on_message_calls_spawn_pod_with_message(self, create_ceph_path, acquire_script, _):
        message = mock.MagicMock()

        self.joc.on_message(message)

        self.joc.job_creator.spawn_job.assert_called_once()
        self.assertIn(
            f"run-{Path(message['filepath']).stem}", self.joc.job_creator.spawn_job.call_args.kwargs["job_name"]
        )
        self.assertEqual(self.joc.job_creator.spawn_job.call_args.kwargs["script"], acquire_script.return_value)
        self.assertEqual(self.joc.job_creator.spawn_job.call_args.kwargs["ceph_path"], create_ceph_path.return_value)

    @mock.patch("job_controller.job_controller.acquire_script")
    @mock.patch("job_controller.job_controller.create_ceph_path")
    def test_on_message_aquires_script_using_filename(self, _, acquire_script):
        message = mock.MagicMock()
        self.joc.ir_api_host = mock.MagicMock()

        self.joc.on_message(message)

        acquire_script.assert_called_once_with(
            filename=os.path.basename(message["filepath"]), ir_api_host=self.joc.ir_api_host
        )

    @mock.patch("job_controller.job_controller.acquire_script")
    @mock.patch("job_controller.job_controller.create_ceph_path")
    def test_on_message_calls_create_ceph_path(self, create_ceph_path, _):
        message = mock.MagicMock()

        self.joc.on_message(message)

        create_ceph_path.assert_called_once_with(
            instrument_name=message["instrument"], rb_number=message["experiment_number"]
        )

    @mock.patch("job_controller.job_controller.acquire_script")
    @mock.patch("job_controller.job_controller.create_ceph_path")
    def test_on_message_sends_the_job_to_the_job_watch(self, create_ceph_path, __):
        message = mock.MagicMock()
        self.joc.create_job_watcher = mock.MagicMock()

        self.joc.on_message(message)

        self.joc.create_job_watcher.assert_called_once_with(
            self.joc.job_creator.spawn_job.return_value, create_ceph_path.return_value
        )

    @mock.patch("job_controller.job_controller.logger")
    def test_ensure_exceptions_are_caught_and_logged_using_the_logger_in_on_message(self, logger):
        exception = Exception("Super cool exception message")
        message = mock.MagicMock()

        def exception_side_effect(*_, **__):
            raise exception

        self.joc.job_creator.spawn_job = mock.MagicMock(side_effect=exception_side_effect)

        self.joc.on_message(message)

        logger.exception.assert_called_once_with(exception)

    @mock.patch("job_controller.job_controller.JobWatcher")
    @mock.patch("job_controller.job_controller.threading")
    def test_create_job_watchers_spins_off_new_thread_with_jobwatcher(self, threading, job_watcher):
        self.joc.create_job_watcher(job_name="job", ceph_path="/path/to/ceph/folder/for/output")

        threading.Thread.assert_called_once_with(target=job_watcher.return_value.watch)
        threading.Thread.return_value.start.assert_called_once_with()

    def test_run_class_starts_consuming(self):
        self.joc.run()

        self.joc.consumer.start_consuming.assert_called_once_with()