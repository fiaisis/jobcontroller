import os
import random
import unittest
from unittest import mock


class MainTest(unittest.TestCase):
    def setUp(self):
        self.max_time_to_complete_job = str(random.randint(1, 60 * 60 * 60))  # noqa: S311
        self.container_name = str(mock.MagicMock())
        self.job_name = str(mock.MagicMock())
        self.pod_name = str(mock.MagicMock())
        self.db_ip = str(mock.MagicMock())
        self.db_username = str(mock.MagicMock())
        self.db_password = str(mock.MagicMock())

        os.environ["MAX_TIME_TO_COMPLETE_JOB"] = self.max_time_to_complete_job
        os.environ["CONTAINER_NAME"] = self.container_name
        os.environ["JOB_NAME"] = self.job_name
        os.environ["POD_NAME"] = self.pod_name
        os.environ["DB_IP"] = self.db_ip
        os.environ["DB_USERNAME"] = self.db_username
        os.environ["DB_PASSWORD"] = self.db_password

    @mock.patch("jobwatcher.main.JobWatcher")
    @mock.patch("jobwatcher.main.load_kubernetes_config")
    def test_main(self, load_kubernetes_config, job_watcher):
        from jobwatcher.main import main

        main()

        load_kubernetes_config.assert_called_once()
        job_watcher.assert_called_once_with(
            max_time_to_complete=int(self.max_time_to_complete_job),
            container_name=self.container_name,
            job_name=self.job_name,
            partial_pod_name=self.pod_name,
        )
        job_watcher.return_value.watch.assert_called_once_with()
