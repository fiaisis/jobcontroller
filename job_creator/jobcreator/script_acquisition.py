"""
Contains the functions for acquiring a script for the job workflow
"""

import time
from http import HTTPStatus
from typing import Any

import requests


def post_autoreduction_job(
    fia_api_host: str, fia_api_key: str, autoreduction_request: dict[str, Any]
) -> tuple[str, int]:
    """
    Given the fia api host, api key, and autoreduction request, return the script and job id of the stored job
    :param fia_api_host: The host of the fia api
    :param fia_api_key: The api key for the fia api
    :param autoreduction_request: The autoreduction request
    :return: Tuple containing the script string and the job id
    """
    retry_attempt, max_attempts = 0, 3
    while retry_attempt <= max_attempts:
        response = requests.post(
            f"http://{fia_api_host}/jobs/autoreduction",
            headers={"Authorization": f"Bearer {fia_api_key}"},
            json=autoreduction_request,
            timeout=30,
        )
        if response.status_code != HTTPStatus.CREATED:
            retry_attempt += 1
            time.sleep(3 + retry_attempt)
            continue
        return response.json().get("script"), response.json()["job_id"]
    raise RuntimeError("Failed to acquire autoreduction script")


def apply_json_output(script: str) -> str:
    """
    The aim is to force whatever the script that is passed to also output to stdinput a json string that consists of
    3 values, status of the run (status), status message, and output files.
    :return: The passed script with 3 lines added to ensure a json dump occurs at the end
    """
    script_addon = (
        "import json\n"
        "\n"
        "print(json.dumps({'status': 'Successful', 'status_message': '', 'output_files': output, 'stacktrace': ''}))\n"
    )
    return script + "\n" + script_addon
