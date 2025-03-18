"""
Watch a kubernetes job, and when it ends update the DB with the results, and exit.
"""

import datetime
import json
import os
import time
from http import HTTPStatus
from json import JSONDecodeError
from time import sleep
from typing import Any, Literal, cast

import requests
from kubernetes import client  # type: ignore[import-untyped]
from kubernetes.client import V1ContainerStatus, V1Job, V1Pod  # type: ignore[import-untyped]

from jobwatcher.utils import logger

StateString = Literal["SUCCESSFUL", "UNSUCCESSFUL", "ERROR", "NOT_STARTED"]
FIA_API_HOST = os.environ.get("FIA_API", "fia-api-service.fia.svc.cluster.local:80")
FIA_API_API_KEY = os.environ.get("FIA_API_API_KEY", "")


def clean_up_pvcs_for_job(job: V1Job, namespace: str) -> None:
    """
    Delete the PVCs associated with the job
    :param namespace: str, the namespace the PVCs of the job are in
    :param job: V1Job, the object whose PVCs need being cleaned up
    :return: None
    """
    v1 = client.CoreV1Api()
    pvcs_to_delete_str = job.metadata.annotations["pvcs"]
    # Clean up the string and turn it into a list
    pvcs_to_delete = pvcs_to_delete_str.strip("][").split(", ")
    logger.info("Deleting pvcs: %s", pvcs_to_delete)
    for pvc in pvcs_to_delete:
        # Strip pv name for ' just in case they have stuck around.
        pvc_name = pvc.strip("'")
        if pvc_name is not None and pvc_name != "None":
            v1.delete_namespaced_persistent_volume_claim(pvc.strip("'").strip('"'), namespace=namespace)
            logger.info("Deleted pv: %s", pvc)


def clean_up_pvs_for_job(job: V1Job) -> None:
    """
    Delete the PVs associated with the job
    :param job: V1Job, the object whose PVs need being cleaned up
    :return: None
    """
    v1 = client.CoreV1Api()
    pvs_to_delete_str = job.metadata.annotations["pvs"]
    # Clean up the string and turn it into a list
    pvs_to_delete = pvs_to_delete_str.strip("][").split(", ")
    logger.info("Deleting pvs: %s", pvs_to_delete)
    for pv in pvs_to_delete:
        # Strip pv name for ' just in case they have stuck around.
        pv_name = pv.strip("'").strip('"')
        if pv_name is not None and pv_name != "None":
            v1.delete_persistent_volume(pv_name)
            logger.info("Deleted pv: %s", pv)


def _find_pod_from_partial_name(partial_pod_name: str, namespace: str) -> V1Pod | None:
    """
    Find a pod from a partial name and it's namespace
    :param partial_pod_name: str, the partial name of the pod
    :param namespace: str, the namespace of the pod
    :return: V1Pod optional, the Pod info if found or None.
    """
    v1 = client.CoreV1Api()
    pods_in_fia = v1.list_namespaced_pod(namespace=namespace)
    for pod in pods_in_fia.items:
        if partial_pod_name in pod.metadata.name:
            return pod
    return None


def _find_latest_raised_error_and_stacktrace_from_reversed_logs(reversed_logs: list[str]) -> tuple[str, str]:
    """
    Find the stacktrace in the logs and then return that as a string, find the line that has the error in it and
    also return that
    :param reversed_logs: list[str], a list of logs in reverse real order so the most recent is at pos 0.
    :return: Tuple[str, str], pos1 contains the error_line, pos2 contains the stacktrace from the logs if one exists
    """
    line_to_record: str = str(reversed_logs[0])  # Last line in the logs (already reversed)
    stacktrace_lines: list[str] = []
    # Find the error line, then record every line
    for line in reversed_logs:
        if not stacktrace_lines:  # Empty list
            if "Error:" in line:
                line_to_record = line
                stacktrace_lines.append(line)
        elif "Traceback (most recent call last):" not in line:
            stacktrace_lines.append(line)
        else:
            # Will contain Traceback
            stacktrace_lines.append(line)
            break
    stacktrace_lines.reverse()  # Correct the incorrect order making "Traceback: ..." first
    stacktrace = ""
    for line in stacktrace_lines:
        stacktrace += line + "\n"
    return line_to_record, stacktrace


class JobWatcher:
    """
    Watch a kubernetes job, and when it ends update the DB with the results, and exit.
    """

    def __init__(
        self,
        job_name: str,
        partial_pod_name: str,
        container_name: str,
        max_time_to_complete: int,
    ) -> None:
        """
        The init for the JobWatcher class
        :param job_name: str, The name of the job to be watched
        :param partial_pod_name: str, the partial name of the pod to be watched
        :param container_name: str, The name of the container you should watch
        :param max_time_to_complete: int, The maximum time before we assume the job is stalled.
        :return: None
        """
        self.namespace = os.environ.get("JOB_NAMESPACE", "fia")
        self.max_time_to_complete = max_time_to_complete
        self.done_watching = False
        self.job_name = job_name
        self.container_name = container_name
        self.job: V1Job | None = None
        self.pod_name: str | None = None
        self.pod: V1Pod | None = None

        self.update_current_container_info(partial_pod_name)

    def watch(self) -> None:
        """
        This is the main function responsible for watching a job, and it's responsible for calling the function that
        will notify the message broker.
        :return: None
        """
        logger.info("Starting job watcher, scanning for new job states.")
        while not self.done_watching:
            self.check_for_changes()
            # Brief sleep to facilitate reducing CPU and network load
            if not self.done_watching:
                logger.info("Container still busy: %s", self.container_name)
                sleep(0.5)

    def update_current_container_info(self, partial_pod_name: str | None = None) -> None:
        """
        Updates the current container info that the job watcher is aware of.
        :param partial_pod_name: optional str, the partial name of the pod that the job is running
        :return: None
        """
        v1 = client.CoreV1Api()
        v1_batch = client.BatchV1Api()
        self.job = v1_batch.read_namespaced_job(self.job_name, namespace=self.namespace)
        if partial_pod_name is not None:
            logger.info("Finding the pod including name: %s", partial_pod_name)
            self.pod = _find_pod_from_partial_name(partial_pod_name, namespace=self.namespace)
            if self.pod is None:
                raise ValueError(f"The pod could not be found using partial pod name: {partial_pod_name}")
            logger.info("Pod found: %s", self.pod.metadata.name)
            self.pod_name = self.pod.metadata.name
        else:
            if self.pod_name is None:
                raise ValueError(
                    "Can't update container info if pod_name was not set and partial_pod_name not provided."
                )
            self.pod = v1.read_namespaced_pod(name=self.pod_name, namespace=self.namespace)

    def check_for_changes(self) -> None:
        """
        Check if the job has a change for which we need to react to, such as the pod
        having finished or a job has stalled.
        :return: None
        """
        self.update_current_container_info()
        if self.check_for_job_complete():
            self.cleanup_job()
            self.done_watching = True
        elif self.check_for_pod_stalled():
            logger.info("Job has stalled out...")
            self.cleanup_job()
            self.done_watching = True

    def get_container_status(self) -> V1ContainerStatus | None:
        """
        Get and return the current container status, ignoring the job watcher's container
        :return: Optional[V1ContainerStatus], The job's main container status
        """
        # Find container
        if self.pod is not None:
            for container_status in self.pod.status.container_statuses:
                if container_status.name == self.container_name:
                    return container_status
        return None

    def check_for_job_complete(self) -> bool:
        """
        Checks if the job has finished by checking its status, if it failed then we
        need to process that, and the same for a success.
        :return: bool, True if job complete, False if job not finished
        """
        container_status = self.get_container_status()
        if container_status is None:
            raise ValueError(f"Container not found: {self.container_name}")
        if container_status.state.terminated is not None:
            # Container has finished
            if container_status.state.terminated.exit_code == 0:
                # Job has succeeded
                logger.info("Job has succeeded... processing success.")
                self.process_job_success()
                return True
            # Job has failed
            logger.info("Job has errored... processing failure.")
            self.process_job_failed()
            return True
        return False

    def check_for_pod_stalled(self) -> bool:
        """
        The way this checks if a job is stalled is by checking if there has been no new
        logs for the last 30 minutes, or if the job has taken over 6 hours to complete.
        Long term 6 hours may be too little so this is configurable using the
        environment variables.
        :return: bool, True if pod is stalled, False if pod is not stalled.
        """
        if self.pod is None:
            raise AttributeError("Pod must be set in the JobWatcher before calling this function.")
        v1_core = client.CoreV1Api()
        seconds_in_30_minutes = 60 * 30
        # If pod is younger than 30 minutes it can't be stalled for 30 minutes, if older, then check.
        if (datetime.datetime.now(datetime.UTC) - self.pod.metadata.creation_timestamp) > datetime.timedelta(
            seconds=seconds_in_30_minutes
        ):
            logs = v1_core.read_namespaced_pod_log(
                name=self.pod.metadata.name,
                namespace=self.pod.metadata.namespace,
                timestamps=True,
                tail_lines=1,
                since_seconds=seconds_in_30_minutes,
                container=self.container_name,
            )
            if logs == "":
                logger.info("No new logs for pod %s in %s seconds", self.pod.metadata.name, seconds_in_30_minutes)
                return True
        if (datetime.datetime.now(datetime.UTC) - self.pod.metadata.creation_timestamp) > datetime.timedelta(
            seconds=self.max_time_to_complete
        ):
            logger.info("Pod has timed out: %s", self.pod.metadata.name)
            return True
        return False

    def _find_start_and_end_of_pod(self, pod: V1Pod) -> tuple[Any, Any | None]:
        """
        Find the start and end of the pod
        :param pod: V1Pod, the pod that the start and end of the pod is meant to be delayed
        :return: Tuple[Any, Optional[Any]], The start datetime, and the optional end datetime if the pod has finished.
        """
        v1_core = client.CoreV1Api()
        pod = v1_core.read_namespaced_pod(pod.metadata.name, self.namespace)
        start_time = pod.status.start_time
        end_time = None
        container_status = self.get_container_status()
        if container_status is not None and container_status.state.terminated:
            end_time = container_status.state.terminated.finished_at
        return start_time, end_time

    def _find_latest_raised_error_and_stacktrace(self) -> tuple[str, str]:
        """
        Find the stacktrace in the logs and then return that as a string, find the line that has the error in it and
        also return that.
        :return: Tuple[str, str], pos1 contains the error_line, pos2 contains the stacktrace from the logs if one exists
        """
        if self.pod is None:
            raise AttributeError("Pod must be set in the JobWatcher before calling this function.")
        v1_core = client.CoreV1Api()
        logs = v1_core.read_namespaced_pod_log(
            name=self.pod.metadata.name,
            namespace=self.pod.metadata.namespace,
            tail_lines=50,
            container=self.container_name,
        ).split("\n")
        logs.reverse()
        return _find_latest_raised_error_and_stacktrace_from_reversed_logs(logs)

    @staticmethod
    def _update_job_status(
        job_id: int,
        state: StateString,
        status_message: str,
        output_files: list[str],
        start: Any,
        stacktrace: str,
        end: str,
    ) -> None:
        retry_attempts, max_attempts = 0, 3
        while retry_attempts <= max_attempts:
            response = requests.patch(
                f"http://{FIA_API_HOST}/job/{job_id}",
                data={
                    "state": state,
                    "status_message": status_message,
                    "output_files": output_files,
                    "start": start,
                    "stacktrace": stacktrace,
                    "end": end,
                },
                headers={"Authorization": f"Bearer {FIA_API_API_KEY}"},
                timeout=30,
            )
            if response.status_code == HTTPStatus.OK:
                return
            retry_attempts += 1
            time.sleep(3 + retry_attempts)
        logger.error("Failed 3 time to contact fia api while updating job status")

    def process_job_failed(self) -> None:
        """
        Process the event that failed, and notify the message broker
        :return: None
        """
        if self.pod is None or self.job is None:
            raise AttributeError("Pod and job must be set in the JobWatcher before calling this function.")
        raised_error, stacktrace = self._find_latest_raised_error_and_stacktrace()
        logger.info("Job %s has failed, with message: %s", self.job.metadata.name, raised_error)
        job_id = self.job.metadata.annotations["job-id"]
        start, end = self._find_start_and_end_of_pod(self.pod)
        self._update_job_status(job_id, "ERROR", raised_error, [], start, stacktrace, str(end))

    def process_job_success(self) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify the message broker
        :return: None
        """
        if self.job is None:
            raise AttributeError("Job must be set in the JobWatcher before calling this function.")
        job_name = self.job.metadata.name
        job_id = self.job.metadata.annotations.get("job-id")
        if self.pod is None:
            raise AttributeError(
                f"Pod name can't be None, {job_name} name and {self.namespace} "
                f"namespace returned None when looking for a pod."
            )
        v1_core = client.CoreV1Api()
        # Convert message from JSON string to python dict
        try:
            logs = v1_core.read_namespaced_pod_log(
                name=self.pod.metadata.name, namespace=self.namespace, container=self.container_name
            )
            log_lines = logs.split("\n")
            # Get second to last line if more than one (last line is empty)
            output = log_lines[-1] if len(log_lines) == 1 else log_lines[-2]
            logger.info("Job %s has been completed with output: %s", job_name, output)
            job_output = json.loads(output)
        except JSONDecodeError as exception:
            logger.error("Last message from job is not a JSON string")
            logger.exception(exception)
            job_output = {
                "status": "UNSUCCESSFUL",
                "output_files": [],
                "status_message": f"{exception!s}",
                "stacktrace": "",
            }
        except TypeError as exception:
            logger.error("Last message from job is not a string: %s", str(exception))
            logger.exception(exception)
            job_output = {
                "status": "UNSUCCESSFUL",
                "output_files": [],
                "status_message": f"{exception!s}",
                "stacktrace": "",
            }
        except Exception as exception:
            logger.error("There was a problem recovering the job output")
            logger.exception(exception)
            job_output = {
                "status": "UNSUCCESSFUL",
                "output_files": [],
                "status_message": f"{exception!s}",
                "stacktrace": "",
            }

        # Grab status from output
        status = cast(StateString, job_output.get("status", "UNSUCCESSFUL").upper())
        status_message = job_output.get("status_message", "")
        stacktrace = job_output.get("stacktrace", "")
        output_files = job_output.get("output_files", [])
        start, end = self._find_start_and_end_of_pod(self.pod)
        self._update_job_status(job_id, status, status_message, output_files, start, stacktrace, str(end))

    def cleanup_job(self) -> None:
        """
        Cleanup the leftovers that a job will leave behind when it cleans up itself
        after a timeout, namely PVs and PVCs
        """
        if self.job is None:
            raise AttributeError("Job must be set in the JobWatcher before calling this function.")
        logger.info("Starting cleanup of job %s", self.job.metadata.name)
        clean_up_pvs_for_job(self.job)
        clean_up_pvcs_for_job(self.job, self.namespace)
