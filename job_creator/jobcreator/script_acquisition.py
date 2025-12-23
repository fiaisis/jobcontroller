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
        try:
            response = requests.post(
                f"https://{fia_api_host}/job/autoreduction",
                headers={"Authorization": f"Bearer {fia_api_key}"},
                json=autoreduction_request,
                timeout=30,
            )
        except requests.exceptions.SSLError as sslerror:
            # If fails to use SSL and using cluster.local as part of host, use http.
            if "cluster.local" in fia_api_host:
                response = requests.post(
                    f"http://{fia_api_host}/job/autoreduction",
                    headers={"Authorization": f"Bearer {fia_api_key}"},
                    json=autoreduction_request,
                    timeout=30,
                )
            else:
                raise sslerror
        if response.status_code != HTTPStatus.CREATED:
            retry_attempt += 1
            time.sleep(3 + retry_attempt)
            continue
        return response.json().get("script"), response.json()["job_id"]
    raise RuntimeError("Failed to acquire autoreduction script")
