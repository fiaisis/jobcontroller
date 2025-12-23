from http import HTTPStatus
from unittest.mock import Mock, call, patch

import pytest

from jobcreator.script_acquisition import post_autoreduction_job


@patch("jobcreator.script_acquisition.time.sleep", autospec=True)
@patch("jobcreator.script_acquisition.requests.post", autospec=True)
def test_immediate_success(mock_post, mock_sleep):
    resp = Mock()
    resp.status_code = HTTPStatus.CREATED
    expected_job_id = 12345
    resp.json.return_value = {"script": "do stuff", "job_id": expected_job_id}
    mock_post.return_value = resp

    script, job_id = post_autoreduction_job("api.host", "KEY", {"foo": "bar"})

    assert script == "do stuff"
    assert job_id == expected_job_id

    mock_post.assert_called_once_with(
        "https://api.host/job/autoreduction",
        headers={"Authorization": "Bearer KEY"},
        json={"foo": "bar"},
        timeout=30,
    )
    mock_sleep.assert_not_called()


@patch("jobcreator.script_acquisition.time.sleep", autospec=True)
@patch("jobcreator.script_acquisition.requests.post", autospec=True)
def test_retries_then_success(mock_post, mock_sleep):
    fail = Mock(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, json=Mock(return_value={}))
    expected_job_id = 999
    expected_post_call_count = 3
    success = Mock(
        status_code=HTTPStatus.CREATED, json=Mock(return_value={"script": "run me", "job_id": expected_job_id})
    )
    mock_post.side_effect = [fail, fail, success]

    script, job_id = post_autoreduction_job("host", "APIKEY", {"a": 1})

    assert script == "run me"
    assert job_id == expected_job_id

    assert mock_post.call_count == expected_post_call_count

    mock_sleep.assert_has_calls([call(4), call(5)], any_order=False)


@patch("jobcreator.script_acquisition.time.sleep", autospec=True)
@patch("jobcreator.script_acquisition.requests.post", autospec=True)
def test_exhausts_retries(mock_post, mock_sleep):
    fail = Mock(status_code=HTTPStatus.BAD_GATEWAY, json=Mock(return_value={}))
    mock_post.side_effect = [fail] * 5
    expected_post_call_count = 4

    with pytest.raises(RuntimeError) as exc:
        post_autoreduction_job("h", "k", {})

    assert "Failed to acquire autoreduction script" in str(exc.value)
    assert mock_post.call_count == expected_post_call_count  # attempts 0,1,2,3 → then raises
    mock_sleep.assert_has_calls([call(4), call(5), call(6)], any_order=False)
