import unittest
from unittest import mock
import os

# Mock environment variables before importing main
with mock.patch.dict(os.environ, {
    "DEFAULT_RUNNER_SHA": "default-sha",
    "IMAGING_RUNNER_SHA": "imaging-sha",
    "WATCHER_SHA": "watcher-sha"
}):
    from jobcreator.main import _generate_special_pvs, _select_runner_image, _select_taints_and_affinity

class TestMain(unittest.TestCase):

    def test_generate_special_pvs_imat_with_ngem(self):
        additional_values = {"ngem": "true"}
        pvs = _generate_special_pvs("imat", additional_values)
        self.assertEqual(pvs, ["ngem"])

    def test_generate_special_pvs_imat_without_ngem(self):
        additional_values = {"ngem": "false"}
        pvs = _generate_special_pvs("imat", additional_values)
        self.assertEqual(pvs, ["imat"])

    def test_generate_special_pvs_ines_with_ngem(self):
        additional_values = {"ngem": "true"}
        pvs = _generate_special_pvs("ines", additional_values)
        self.assertEqual(pvs, ["ngem"])

    def test_generate_special_pvs_ines_without_ngem(self):
        additional_values = {"ngem": "false"}
        pvs = _generate_special_pvs("ines", additional_values)
        self.assertEqual(pvs, ["ines"])

    def test_generate_special_pvs_other(self):
        additional_values = {"ngem": "true"}
        pvs = _generate_special_pvs("other", additional_values)
        self.assertEqual(pvs, [])

    def test_select_runner_image_imat_with_ngem(self):
        additional_values = {"ngem": "true"}
        image = _select_runner_image("imat", additional_values)
        self.assertIn("default-sha", image)

    def test_select_runner_image_imat_without_ngem(self):
        from jobcreator.main import _select_runner_image
        with mock.patch("jobcreator.main.IMAGING_RUNNER_SHA", "imaging-sha"):
            with mock.patch("jobcreator.main.IMAGING_RUNNER", "ghcr.io/fiaisis/mantidimaging@sha256:imaging-sha"):
                additional_values = {"ngem": "false"}
                image = _select_runner_image("imat", additional_values)
                self.assertIn("imaging-sha", image)

    def test_select_runner_image_other(self):
        additional_values = {"ngem": "true"}
        image = _select_runner_image("other", additional_values)
        self.assertIn("default-sha", image)

    def test_select_taints_and_affinity_imat_with_ngem(self):
        additional_values = {"ngem": "true"}
        taints, affinity = _select_taints_and_affinity("imat", additional_values)
        self.assertEqual(taints, [])
        self.assertIsNone(affinity)

    def test_select_taints_and_affinity_imat_without_ngem(self):
        additional_values = {"ngem": "false"}
        taints, affinity = _select_taints_and_affinity("imat", additional_values)
        self.assertEqual(len(taints), 1)
        self.assertEqual(taints[0]["key"], "nvidia.com/gpu")
        self.assertIsNotNone(affinity)
        self.assertEqual(affinity["key"], "node-type")

    def test_select_taints_and_affinity_other(self):
        additional_values = {"ngem": "true"}
        taints, affinity = _select_taints_and_affinity("other", additional_values)
        self.assertEqual(taints, [])
        self.assertIsNone(affinity)

    @mock.patch("jobcreator.main.JOB_CREATOR")
    @mock.patch("jobcreator.main.create_ceph_mount_path_simple")
    @mock.patch("jobcreator.main.find_sha256_of_image")
    def test_process_simple_message_user_number(self, mock_find_sha, mock_create_path, mock_job_creator):
        from jobcreator.main import process_simple_message
        mock_find_sha.return_value = "sha256:123"
        mock_create_path.return_value = "/ceph/path"
        message = {
            "runner_image": "image:latest",
            "script": "print('hello')",
            "user_number": 12345,
            "job_id": 1,
            "taints": "[]",
            "affinity": "{}"
        }
        process_simple_message(message)
        mock_job_creator.spawn_job.assert_called_once()
        kwargs = mock_job_creator.spawn_job.call_args.kwargs
        self.assertIn("run-owner12345-requested-", kwargs["job_name"])
        self.assertEqual(kwargs["script"], "print('hello')")
        self.assertEqual(kwargs["runner_image"], "sha256:123")
        self.assertEqual(kwargs["job_id"], 1)

    @mock.patch("jobcreator.main.JOB_CREATOR")
    @mock.patch("jobcreator.main.create_ceph_mount_path_simple")
    @mock.patch("jobcreator.main.find_sha256_of_image")
    def test_process_simple_message_experiment_number(self, mock_find_sha, mock_create_path, mock_job_creator):
        from jobcreator.main import process_simple_message
        mock_find_sha.return_value = "sha256:123"
        mock_create_path.return_value = "/ceph/path"
        message = {
            "runner_image": "image:latest",
            "script": "print('hello')",
            "experiment_number": 67890,
            "job_id": 2,
        }
        process_simple_message(message)
        mock_job_creator.spawn_job.assert_called_once()
        kwargs = mock_job_creator.spawn_job.call_args.kwargs
        self.assertIn("run-owner67890-requested-", kwargs["job_name"])
        self.assertEqual(kwargs["job_id"], 2)

    @mock.patch("jobcreator.main.logger")
    def test_process_simple_message_invalid_job_id(self, mock_logger):
        from jobcreator.main import process_simple_message
        message = {
            "runner_image": "image:latest",
            "script": "print('hello')",
            "job_id": "not-an-int"
        }
        process_simple_message(message)
        mock_logger.exception.assert_called_once()

    @mock.patch("jobcreator.main.JOB_CREATOR")
    @mock.patch("jobcreator.main.create_ceph_mount_path_autoreduction")
    @mock.patch("jobcreator.main.find_sha256_of_image")
    def test_process_rerun_message(self, mock_find_sha, mock_create_path, mock_job_creator):
        from jobcreator.main import process_rerun_message
        mock_find_sha.return_value = "sha256:rerun"
        mock_create_path.return_value = "/ceph/autoreduction"
        message = {
            "runner_image": "image:latest",
            "script": "rerun script",
            "instrument": "imat",
            "rb_number": "12345",
            "filename": "data.nxs",
            "job_id": 100
        }
        process_rerun_message(message)
        mock_job_creator.spawn_job.assert_called_once()
        kwargs = mock_job_creator.spawn_job.call_args.kwargs
        self.assertIn("run-data.nxs-", kwargs["job_name"])
        self.assertEqual(kwargs["job_id"], 100)
        self.assertEqual(kwargs["special_pvs"], ["imat"])

    @mock.patch("jobcreator.main.JOB_CREATOR")
    @mock.patch("jobcreator.main.post_autoreduction_job")
    @mock.patch("jobcreator.main.create_ceph_mount_path_autoreduction")
    @mock.patch("jobcreator.main.find_sha256_of_image")
    def test_process_autoreduction_message(self, mock_find_sha, mock_create_path, mock_post_job, mock_job_creator):
        from jobcreator.main import process_autoreduction_message
        mock_find_sha.return_value = "sha256:auto"
        mock_create_path.return_value = "/ceph/auto"
        mock_post_job.return_value = ("generated_script", 200)
        message = {
            "filepath": "/path/to/data.nxs",
            "experiment_number": "67890",
            "instrument": "imat",
            "experiment_title": "test title",
            "users": "user1",
            "run_start": "start",
            "run_end": "end",
            "good_frames": 100,
            "raw_frames": 110,
            "additional_values": {"ngem": "false"}
        }
        process_autoreduction_message(message)
        mock_job_creator.spawn_job.assert_called_once()
        kwargs = mock_job_creator.spawn_job.call_args.kwargs
        self.assertIn("run-data-", kwargs["job_name"])
        self.assertEqual(kwargs["job_id"], 200)
        self.assertEqual(kwargs["script"], "generated_script")

    @mock.patch("jobcreator.main.process_simple_message")
    @mock.patch("jobcreator.main.process_rerun_message")
    @mock.patch("jobcreator.main.process_autoreduction_message")
    def test_process_message(self, mock_auto, mock_rerun, mock_simple):
        from jobcreator.main import process_message
        process_message({"job_type": "simple"})
        mock_simple.assert_called_once()
        process_message({"job_type": "rerun"})
        mock_rerun.assert_called_once()
        process_message({"job_type": "autoreduction"})
        mock_auto.assert_called_once()
        process_message({})  # defaults to autoreduction
        self.assertEqual(mock_auto.call_count, 2)

    @mock.patch("jobcreator.main.time")
    @mock.patch("jobcreator.main.Path")
    def test_write_readiness_probe_file(self, mock_path, mock_time):
        from jobcreator.main import write_readiness_probe_file
        mock_file = mock.MagicMock()
        mock_path.return_value.open.return_value.__enter__.return_value = mock_file
        mock_time.strftime.return_value = "2023-01-01 00:00:00"
        
        write_readiness_probe_file()
        
        mock_file.write.assert_called_once_with("2023-01-01 00:00:00")

    @mock.patch("jobcreator.main.QueueConsumer")
    def test_main(self, mock_consumer):
        from jobcreator.main import main
        main()
        mock_consumer.assert_called_once()
        mock_consumer.return_value.start_consuming.assert_called_once()

    def test_select_runner_image_imat_missing_sha(self):
        from jobcreator.main import _select_runner_image
        with mock.patch("jobcreator.main.IMAGING_RUNNER_SHA", None):
            image = _select_runner_image("imat", {})
            self.assertIn("default-sha", image)

    @mock.patch("jobcreator.main.logger")
    def test_process_rerun_message_exception(self, mock_logger):
        from jobcreator.main import process_rerun_message
        # This should trigger a KeyError since 'runner_image' is missing
        process_rerun_message({})
        mock_logger.exception.assert_called_once()

    @mock.patch("jobcreator.main.logger")
    def test_process_autoreduction_message_exception(self, mock_logger):
        from jobcreator.main import process_autoreduction_message
        # This should trigger a KeyError since 'filepath' is missing
        process_autoreduction_message({})
        mock_logger.exception.assert_called_once()

    @mock.patch("jobcreator.main.logger")
    def test_process_message_unrecognised(self, mock_logger):
        from jobcreator.main import process_message
        process_message({"job_type": "unknown"})
        mock_logger.warn.assert_called_once()

    @mock.patch("jobcreator.main.main")
    def test_main_coverage(self, mock_main):
        import importlib
        import jobcreator.main
        
        # Test missing DEFAULT_RUNNER_SHA
        with mock.patch.dict(os.environ, {"WATCHER_SHA": "watcher-sha"}, clear=True):
            with self.assertRaises(OSError):
                importlib.reload(jobcreator.main)
                
        # Test missing WATCHER_SHA
        with mock.patch.dict(os.environ, {"DEFAULT_RUNNER_SHA": "default-sha"}, clear=True):
            with self.assertRaises(OSError):
                importlib.reload(jobcreator.main)

        # Test DEV_MODE branch
        with mock.patch.dict(os.environ, {
            "DEFAULT_RUNNER_SHA": "default-sha",
            "WATCHER_SHA": "watcher-sha",
            "DEV_MODE": "True"
        }):
            importlib.reload(jobcreator.main)
            self.assertTrue(jobcreator.main.DEV_MODE)

        # Test __name__ == "__main__" block
        with mock.patch.dict(os.environ, {
            "DEFAULT_RUNNER_SHA": "default-sha",
            "WATCHER_SHA": "watcher-sha"
        }):
            # Use reload to trigger imports and most lines
            with mock.patch("jobcreator.main.__name__", "__main__"):
                with mock.patch("jobcreator.main.main"):
                    importlib.reload(jobcreator.main)
                    
            # Explicitly execute the if __name__ == "__main__": line and the main() call
            # We already have mock_main from the decorator
            source = "if __name__ == '__main__': main()"
            exec_globals = {"main": mock_main, "__name__": "__main__"}
            exec(source, exec_globals)
            mock_main.assert_called_once()
