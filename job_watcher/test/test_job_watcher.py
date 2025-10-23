import os
import random
from datetime import UTC, datetime, timedelta
from http import HTTPStatus
from json import JSONDecodeError
from unittest import mock
from unittest.mock import Mock, call, patch

import pytest

from jobwatcher.job_watcher import (
    JobWatcher,
    _find_latest_raised_error_and_stacktrace_from_reversed_logs,
    _find_pod_from_partial_name,
    clean_up_pvcs_for_job,
    clean_up_pvs_for_job,
)

JOB_NAME = mock.MagicMock()
PARTIAL_POD_NAME = mock.MagicMock()
CONTAINER_NAME = mock.MagicMock()
DB_UPDATER = mock.MagicMock()
MAX_TIME_TO_COMPLETE = mock.MagicMock()


@pytest.fixture
def job_watcher_maker():
    with (
        mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name") as find_pod_from_partial_name_mock,
        mock.patch("jobwatcher.job_watcher.client") as client,
    ):
        jw = JobWatcher(
            JOB_NAME,
            PARTIAL_POD_NAME,
            CONTAINER_NAME,
            MAX_TIME_TO_COMPLETE,
        )
        return jw, find_pod_from_partial_name_mock, client


def test_clean_up_pvcs_for_job():
    namespace = str(mock.MagicMock())
    job = mock.MagicMock()
    pvcs = [str(mock.MagicMock()), str(mock.MagicMock()), str(mock.MagicMock())]
    job.metadata.annotations = {"pvcs": str(pvcs)}

    with mock.patch("jobwatcher.job_watcher.client") as client:
        clean_up_pvcs_for_job(job, namespace)

    for pvc in pvcs:
        assert (
            call(pvc, namespace=namespace)
            in client.CoreV1Api.return_value.delete_namespaced_persistent_volume_claim.call_args_list
        )


@mock.patch("jobwatcher.job_watcher.client")
def test_clean_up_pvs_for_job(client):
    job = mock.MagicMock()
    pvs = [str(mock.MagicMock()), str(mock.MagicMock()), str(mock.MagicMock())]
    job.metadata.annotations = {"pvs": str(pvs)}

    clean_up_pvs_for_job(job)

    for pv in pvs:
        assert call(pv) in client.CoreV1Api.return_value.delete_persistent_volume.call_args_list


@mock.patch("jobwatcher.job_watcher.client")
def test_find_pod_from_partial_name(client):
    partial_name = str(mock.MagicMock())
    namespace = str(mock.MagicMock())
    pod = mock.MagicMock()
    pod.metadata.name = partial_name + "123123123"
    client.CoreV1Api.return_value.list_namespaced_pod.return_value.items = [pod]

    assert pod == _find_pod_from_partial_name(partial_name, namespace)

    client.CoreV1Api.return_value.list_namespaced_pod.assert_called_once_with(namespace=namespace)


@mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name")
@mock.patch("jobwatcher.job_watcher.client")
def test_jobwatcher_init(client, find_pod_from_partial_name):
    job_name = str(mock.MagicMock())
    partial_pod_name = str(mock.MagicMock())
    container_name = str(mock.MagicMock())
    max_time_to_complete = random.randint(1, 21000)  # noqa: S311
    namespace = str(mock.MagicMock())
    os.environ["JOB_NAMESPACE"] = namespace

    jw = JobWatcher(job_name, partial_pod_name, container_name, max_time_to_complete)

    assert jw.job_name == job_name
    assert jw.pod_name == find_pod_from_partial_name.return_value.metadata.name
    assert jw.pod == find_pod_from_partial_name.return_value
    assert jw.job == client.BatchV1Api.return_value.read_namespaced_job()
    assert jw.done_watching is False
    assert jw.max_time_to_complete == max_time_to_complete
    assert jw.namespace == namespace


@pytest.mark.usefixtures("job_watcher_maker")
def test_jobwatcher_watch_done_watching(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    jw.check_for_changes = mock.MagicMock()
    jw.done_watching = True

    jw.watch()

    jw.check_for_changes.assert_not_called()


@pytest.mark.usefixtures("job_watcher_maker")
def test_jobwatcher_watch_not_done_watching_but_done_via_changes(job_watcher_maker):
    jw, _, __ = job_watcher_maker

    def done_watching():
        jw.done_watching = True

    jw.check_for_changes = mock.MagicMock(side_effect=done_watching)
    jw.done_watching = False

    jw.watch()

    jw.check_for_changes.assert_called_once_with()


@pytest.mark.usefixtures("job_watcher_maker")
def test_update_current_container_info_no_name_provided(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    jw.pod_name = None

    with pytest.raises(ValueError), mock.patch("jobwatcher.job_watcher.client"):  # noqa: PT011
        jw.update_current_container_info()


@pytest.mark.usefixtures("job_watcher_maker")
def test_update_current_container_info_full_name(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    pod_name = str(mock.MagicMock())
    jw.pod_name = pod_name

    with mock.patch("jobwatcher.job_watcher.client") as client:
        jw.update_current_container_info()

    client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(name=pod_name, namespace=jw.namespace)
    assert jw.pod == client.CoreV1Api.return_value.read_namespaced_pod.return_value


@pytest.mark.usefixtures("job_watcher_maker")
def test_update_current_container_info_partial_name(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    partial_pod_name = str(mock.MagicMock())
    jw.pod_name = None

    with (
        mock.patch("jobwatcher.job_watcher._find_pod_from_partial_name") as _find_pod_from_partial_name,
        mock.patch("jobwatcher.job_watcher.client"),
    ):
        jw.update_current_container_info(partial_pod_name)

    assert jw.pod == _find_pod_from_partial_name.return_value
    _find_pod_from_partial_name.assert_called_with(partial_pod_name, namespace=jw.namespace)
    assert _find_pod_from_partial_name.call_count == 1


@pytest.mark.usefixtures("job_watcher_maker")
def test_update_current_container_info_partial_name_no_pod(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    partial_name = str(mock.MagicMock())

    with (
        pytest.raises(ValueError),  # noqa: PT011
        mock.patch(
            "jobwatcher.job_watcher._find_pod_from_partial_name", return_value=None
        ) as _find_pod_from_partial_name,
        mock.patch("jobwatcher.job_watcher.client"),
    ):
        jw.update_current_container_info(partial_name)


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_changes_job_incomplete_and_not_stalled(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.update_current_container_info = mock.MagicMock()
    jw.cleanup_job = mock.MagicMock()
    jw.check_for_job_complete = mock.MagicMock(return_value=False)
    jw.check_for_pod_stalled = mock.MagicMock(return_value=False)
    jw.done_watching = False

    jw.check_for_changes()

    jw.update_current_container_info.assert_called_once_with()
    jw.cleanup_job.assert_not_called()
    jw.check_for_job_complete.assert_called_once_with()
    jw.check_for_pod_stalled.assert_called_once_with()
    assert jw.done_watching is False


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_changes_job_complete_and_not_stalled(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.update_current_container_info = mock.MagicMock()
    jw.cleanup_job = mock.MagicMock()
    jw.check_for_job_complete = mock.MagicMock(return_value=True)
    jw.check_for_pod_stalled = mock.MagicMock(return_value=False)
    jw.done_watching = False

    jw.check_for_changes()

    jw.update_current_container_info.assert_called_once_with()
    jw.cleanup_job.assert_called_once_with()
    jw.check_for_job_complete.assert_called_once_with()
    jw.check_for_pod_stalled.assert_not_called()
    assert jw.done_watching is True


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_changes_job_complete_and_stalled(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.update_current_container_info = mock.MagicMock()
    jw.cleanup_job = mock.MagicMock()
    jw.check_for_job_complete = mock.MagicMock(return_value=True)
    jw.check_for_pod_stalled = mock.MagicMock(return_value=True)
    jw.done_watching = False

    jw.check_for_changes()

    jw.update_current_container_info.assert_called_once_with()
    jw.cleanup_job.assert_called_once_with()
    jw.check_for_job_complete.assert_called_once_with()
    jw.check_for_pod_stalled.assert_not_called()
    assert jw.done_watching is True


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_changes_job_incomplete_and_stalled(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.update_current_container_info = mock.MagicMock()
    jw.cleanup_job = mock.MagicMock()
    jw.check_for_job_complete = mock.MagicMock(return_value=False)
    jw.check_for_pod_stalled = mock.MagicMock(return_value=True)
    jw.done_watching = False

    jw.check_for_changes()

    jw.update_current_container_info.assert_called_once_with()
    jw.cleanup_job.assert_called_once_with()
    jw.check_for_job_complete.assert_called_once_with()
    jw.check_for_pod_stalled.assert_called_once_with()
    assert jw.done_watching is True


@pytest.mark.usefixtures("job_watcher_maker")
def test_get_container_status(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    container_status = mock.MagicMock()
    jw.pod.status.container_statuses = [container_status]
    container_name = str(mock.MagicMock())
    jw.container_name = container_name
    container_status.name = container_name

    result = jw.get_container_status()

    assert result == container_status


@pytest.mark.usefixtures("job_watcher_maker")
def test_get_container_status_pod_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = mock.MagicMock()

    result = jw.get_container_status()

    assert result is None


@pytest.mark.usefixtures("job_watcher_maker")
def test_get_container_status_not_present(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    container_status = mock.MagicMock()
    jw.pod.status.container_statuses = [container_status]
    container_name = str(mock.MagicMock())
    jw.container_name = mock.MagicMock()
    container_status.name = container_name

    result = jw.get_container_status()

    assert result is None


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_job_complete_container_status_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.get_container_status = mock.MagicMock(return_value=None)

    with pytest.raises(ValueError):  # noqa: PT011
        jw.check_for_job_complete()


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_job_complete_container_status_is_terminated_exit_code_0(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    container_status = mock.MagicMock()
    container_status.state.terminated.exit_code = 0
    jw.get_container_status = mock.MagicMock(return_value=container_status)
    jw.process_job_success = mock.MagicMock()
    jw.process_job_failed = mock.MagicMock()

    assert jw.check_for_job_complete() is True

    jw.process_job_success.assert_called_once_with()
    jw.process_job_failed.assert_not_called()


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_job_complete_container_status_is_terminated_exit_code_not_0(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    container_status = mock.MagicMock()
    container_status.state.terminated.exit_code = 100
    jw.get_container_status = mock.MagicMock(return_value=container_status)
    jw.process_job_success = mock.MagicMock()
    jw.process_job_failed = mock.MagicMock()

    assert jw.check_for_job_complete() is True

    jw.process_job_success.assert_not_called()
    jw.process_job_failed.assert_called_once_with()


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_job_complete_container_status_running(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    container_status = mock.MagicMock()
    container_status.state.terminated = None
    jw.get_container_status = mock.MagicMock(return_value=container_status)
    jw.process_job_success = mock.MagicMock()
    jw.process_job_failed = mock.MagicMock()

    assert jw.check_for_job_complete() is False

    jw.process_job_success.assert_not_called()
    jw.process_job_failed.assert_not_called()


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_pod_stalled_pod_is_younger_than_30_minutes(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod.metadata.creation_timestamp = datetime.now(UTC)
    jw.max_time_to_complete = 99999999

    assert jw.check_for_pod_stalled() is False


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_pod_stalled_pod_where_pod_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = None

    with pytest.raises(AttributeError):
        jw.check_for_pod_stalled()


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_pod_stalled_pod_is_stalled_for_30_minutes(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    jw.pod.metadata.creation_timestamp = datetime.now(UTC) - timedelta(seconds=60 * 35)
    jw.max_time_to_complete = 99999999

    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = ""
        assert jw.check_for_pod_stalled() is True

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name,
        namespace=jw.pod.metadata.namespace,
        timestamps=True,
        tail_lines=1,
        since_seconds=60 * 30,
        container=jw.container_name,
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_check_for_pod_stalled_pod_is_running_fine(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod.metadata.creation_timestamp = datetime.now(UTC) - timedelta(seconds=60 * 10)
    jw.max_time_to_complete = 1

    assert jw.check_for_pod_stalled() is True

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_not_called()


@pytest.mark.usefixtures("job_watcher_maker")
def test_find_start_and_end_of_pod(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    pod = mock.MagicMock()
    jw.get_container_status = mock.MagicMock()
    jw.get_container_status.return_value.state.terminated = None

    with mock.patch("jobwatcher.job_watcher.client") as client:
        assert jw._find_start_and_end_of_pod(pod) == (
            client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time,
            None,
        )

    client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(pod.metadata.name, jw.namespace)


@pytest.mark.usefixtures("job_watcher_maker")
def test_find_start_and_end_of_pod_terminated(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    pod = mock.MagicMock()
    jw.get_container_status = mock.MagicMock()

    with mock.patch("jobwatcher.job_watcher.client") as client:
        assert jw._find_start_and_end_of_pod(pod) == (
            client.CoreV1Api.return_value.read_namespaced_pod.return_value.status.start_time,
            jw.get_container_status.return_value.state.terminated.finished_at,
        )

    client.CoreV1Api.return_value.read_namespaced_pod.assert_called_once_with(pod.metadata.name, jw.namespace)


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_failed(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    status_message = mock.MagicMock()
    stacktrace = mock.MagicMock()
    jw._find_latest_raised_error_and_stacktrace = mock.MagicMock(return_value=[status_message, stacktrace])
    start = mock.MagicMock()
    end = mock.MagicMock()
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()
    jw.process_job_failed()

    jw._update_job_status.assert_called_once_with(
        jw.job.metadata.annotations["job-id"],
        "ERROR",
        status_message,
        [],
        str(start),
        stacktrace,
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_failed_pod_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = None
    jw.job = mock.MagicMock()

    with pytest.raises(AttributeError):
        jw.process_job_failed()


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_failed_job_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = mock.MagicMock()
    jw.job = None

    with pytest.raises(AttributeError):
        jw.process_job_failed()


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success(job_watcher_maker):
    jw, _, __ = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._update_job_status = mock.MagicMock()

    with mock.patch("jobwatcher.job_watcher.client") as client:
        logs = """
        line 1
        line 2
        line 3

        {"status": "Successful", "status_message": "status_message", "output_files": "output_file.nxs"}
        """
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = logs
        jw.process_job_success()

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
    )
    jw._update_job_status.assert_called_once_with(
        job_id,
        "SUCCESSFUL",
        "status_message",
        "output_file.nxs",
        str(start),
        "",
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success_pod_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = None
    jw.job = mock.MagicMock()

    with pytest.raises(AttributeError):
        jw.process_job_success()


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success_job_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.job = None
    jw.pod = mock.MagicMock()

    with pytest.raises(AttributeError):
        jw.process_job_success()


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success_raise_json_decode_error(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()

    def raise_error(name, namespace, container):
        raise JSONDecodeError("", "", 1)

    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)
        jw.process_job_success()

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
    )
    jw._update_job_status.assert_called_once_with(
        job_id,
        "UNSUCCESSFUL",
        ": line 1 column 2 (char 1)",
        [],
        str(start),
        "",
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success_raise_type_error(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()

    def raise_error(name, namespace, container):
        raise TypeError("TypeError!")

    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)
        jw.process_job_success()

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
    )
    jw._update_job_status.assert_called_once_with(
        job_id,
        "UNSUCCESSFUL",
        "TypeError!",
        [],
        str(start),
        "",
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_process_job_success_raise_exception(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()

    def raise_error(name, namespace, container):
        raise Exception("Exception raised!")

    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log = mock.MagicMock(side_effect=raise_error)
        jw.process_job_success()

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name, namespace=jw.namespace, container=jw.container_name
    )
    jw._update_job_status.assert_called_once_with(
        job_id,
        "UNSUCCESSFUL",
        "Exception raised!",
        [],
        str(start),
        "",
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_cleanup_job(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker

    with (
        mock.patch("jobwatcher.job_watcher.clean_up_pvs_for_job") as clean_up_pvs_for_job,
        mock.patch("jobwatcher.job_watcher.clean_up_pvcs_for_job") as clean_up_pvcs_for_job,
    ):
        jw.cleanup_job()

    clean_up_pvs_for_job.assert_called_once_with(jw.job)
    clean_up_pvcs_for_job.assert_called_once_with(jw.job, jw.namespace)


@pytest.mark.usefixtures("job_watcher_maker")
def test_cleanup_job_where_job_is_none(job_watcher_maker):
    jw, _find_pod_from_partial_name, client = job_watcher_maker
    jw.job = None

    with pytest.raises(AttributeError):
        jw._cleanup_job()


def test_find_latest_raised_error_and_stacktrace_from_reversed_logs():
    logs = [
        "Random error ahead! Just doing some data processing watch out!",
        "Some data processing output",
        "Traceback (most recent call last):",
        '  File "/path/to/example.py", line 4, in <module>',
        "    processData('I'm processing data over here!')",
        '  File "/path/to/example.py", line 2, in greet',
        "    dataCreate('Give me the gabagool')",
        "AttributeError: I don't know where the gabagool is Tony",
        "{{output the failure to get gabagool using json}}",
    ]
    logs.reverse()
    expected_stacktrace = (
        "Traceback (most recent call last):\n"
        '  File "/path/to/example.py", line 4, in <module>\n'
        "    processData('I'm processing data over here!')\n"
        '  File "/path/to/example.py", line 2, in greet\n'
        "    dataCreate('Give me the gabagool')\n"
        "AttributeError: I don't know where the gabagool is Tony\n"
    )
    expected_recorded_line = "AttributeError: I don't know where the gabagool is Tony"

    with mock.patch("jobwatcher.job_watcher.client"):
        recorded_line, stacktrace = _find_latest_raised_error_and_stacktrace_from_reversed_logs(logs)

    assert recorded_line == expected_recorded_line
    assert stacktrace == expected_stacktrace


@pytest.mark.usefixtures("job_watcher_maker")
def test_find_latest_raised_error_and_stacktrace(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker

    with (
        mock.patch(
            "jobwatcher.job_watcher._find_latest_raised_error_and_stacktrace_from_reversed_logs"
        ) as return_raised_error_call,
        mock.patch("jobwatcher.job_watcher.client") as client,
    ):
        jw._find_latest_raised_error_and_stacktrace()

    client.CoreV1Api.return_value.read_namespaced_pod_log.assert_called_once_with(
        name=jw.pod.metadata.name,
        namespace=jw.pod.metadata.namespace,
        tail_lines=50,
        container=jw.container_name,
    )
    (
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value.split.return_value.reverse.assert_called_once_with()
    )
    return_raised_error_call.assert_called_once_with(
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value.split.return_value
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_find_latest_raised_error_and_stacktrace_raises_error_on_pod_is_none(job_watcher_maker):
    jw, client, _find_pod_from_partial_name = job_watcher_maker
    jw.pod = None

    with pytest.raises(AttributeError):
        jw._find_latest_raised_error_and_stacktrace()


@pytest.mark.usefixtures("job_watcher_maker")
def test_handle_logs_output_only_1_line(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()
    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            '{"status": "Successful", "output_files": "Great files, the best!"}'
        )
        jw.process_job_success()

    jw._update_job_status.assert_called_once_with(
        job_id,
        "SUCCESSFUL",
        "",
        "Great files, the best!",
        str(start),
        "",
        str(end),
    )


@pytest.mark.usefixtures("job_watcher_maker")
def test_handle_logs_output_2_lines(job_watcher_maker):
    jw, _, _find_pod_from_partial_name = job_watcher_maker
    start = mock.MagicMock()
    end = mock.MagicMock()
    job_id = mock.MagicMock()
    jw.job.metadata.annotations.get.return_value = job_id
    jw._find_start_and_end_of_pod = mock.MagicMock(return_value=(start, end))
    jw._update_job_status = mock.MagicMock()
    with mock.patch("jobwatcher.job_watcher.client") as client:
        client.CoreV1Api.return_value.read_namespaced_pod_log.return_value = (
            'Crazy python script logs here!\n{"status": "Successful", "output_files": "Great files, the best!"}\n'
            "Not sure why K8s adds me here!, only sometimes!"
        )
        jw.process_job_success()

    jw._update_job_status.assert_called_once_with(
        job_id,
        "SUCCESSFUL",
        "",
        "Great files, the best!",
        str(start),
        "",
        str(end),
    )


@patch("jobwatcher.job_watcher.FIA_API_HOST", "example.com")
@patch("jobwatcher.job_watcher.FIA_API_API_KEY", "shh")
@patch("requests.patch")
def test_update_job_status_success(mock_patch):
    mock_response = Mock()
    mock_response.status_code = HTTPStatus.OK
    mock_patch.return_value = mock_response

    JobWatcher._update_job_status(
        job_id=1,
        state="SUCCESSFUL",
        status_message="Job done",
        output_files=["file1.txt"],
        start="2025-03-17T10:00:00Z",
        stacktrace="",
        end="2025-03-17T10:05:00Z",
    )

    mock_patch.assert_called_once()
    mock_patch.assert_called_with(
        "http://example.com/job/1",
        json={
            "state": "SUCCESSFUL",
            "status_message": "Job done",
            "outputs": "['file1.txt']",
            "start": "2025-03-17T10:00:00Z",
            "stacktrace": "",
            "end": "2025-03-17T10:05:00Z",
        },
        headers={"Authorization": "Bearer shh"},
        timeout=30,
    )


@patch("requests.patch")
@patch("time.sleep")
def test_update_job_status_retry_success(mock_sleep, mock_patch):
    mock_sleep.return_value = None
    # First two attempts fail, third succeeds
    mock_response_fail = Mock(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
    mock_response_success = Mock(status_code=HTTPStatus.OK)
    mock_patch.side_effect = [mock_response_fail, mock_response_fail, mock_response_success]
    expected_patch_call_count = 3
    JobWatcher._update_job_status(
        job_id=2,
        state="SUCCESSFUL",
        status_message="In progress",
        output_files=[],
        start="2025-03-17T09:00:00Z",
        stacktrace="",
        end="",
    )

    assert mock_patch.call_count == expected_patch_call_count
    mock_sleep.assert_called_with(3 + 2)  # Should sleep after second fail (retry_attempts=2)


@patch("requests.patch")
@patch("time.sleep")  # Avoid sleep
@patch("jobwatcher.job_watcher.logger.critical")
def test_update_job_status_fail(mock_logger_critical, mock_sleep, mock_patch):
    mock_response_fail = Mock(status_code=HTTPStatus.INTERNAL_SERVER_ERROR)
    mock_sleep.return_value = None
    mock_patch.return_value = mock_response_fail
    expected_patch_call_count = 4
    expected_exit_code = 1
    with pytest.raises(SystemExit) as exc:  # noqa: PT012 # Needed for assert of status code
        JobWatcher._update_job_status(
            job_id=3,
            state="SUCCESSFUL",
            status_message="Error occurred",
            output_files=[],
            start="2025-03-17T08:00:00Z",
            stacktrace="Traceback info",
            end="2025-03-17T08:10:00Z",
        )
        assert exc.value.code == expected_exit_code

    assert mock_patch.call_count == expected_patch_call_count
    mock_logger_critical.assert_called_once_with("Failed 3 time to contact fia api while updating job status")
