"""
Watch a kubernetes job, and when it ends update the DB with the results, and exit.
"""

import datetime
import json
import os
from json import JSONDecodeError
from time import sleep
from typing import Any, Optional, Tuple

from kubernetes import client  # type: ignore[import]
from kubernetes.client import V1Job, V1Pod, V1ContainerStatus  # type: ignore[import]

from jobwatcher.database.state_enum import State
from jobwatcher.database.db_updater import DBUpdater
from jobwatcher.utils import logger


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


def _find_pod_from_partial_name(partial_pod_name: str, namespace: str) -> Optional[V1Pod]:
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


class JobWatcher:  # pylint: disable=too-many-instance-attributes
    """
    Watch a kubernetes job, and when it ends update the DB with the results, and exit.
    """

    def __init__(
        self,
        job_name: str,
        partial_pod_name: str,
        container_name: str,
        db_updater: DBUpdater,
        max_time_to_complete: int,
    ) -> None:
        """
        The init for the JobWatcher class
        :param job_name: str, The name of the job to be watched
        :param partial_pod_name: str, the partial name of the pod to be watched
        :param container_name: str, The name of the container you should watch
        :param db_updater: DBUpdater, the DBUpdater to be used for updating the db based on the status of the job
        :param max_time_to_complete: int, The maximum time before we assume the job is stalled.
        :return: None
        """
        self.namespace = os.environ.get("JOB_NAMESPACE", "fia")
        self.db_updater = db_updater
        self.max_time_to_complete = max_time_to_complete
        self.done_watching = False
        self.job_name = job_name
        self.container_name = container_name
        self.job: Optional[V1Job] = None
        self.pod_name: Optional[str] = None
        self.pod: Optional[V1Pod] = None

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

    def update_current_container_info(self, partial_pod_name: Optional[str] = None) -> None:
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

    def get_container_status(self) -> Optional[V1ContainerStatus]:
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
        if (datetime.datetime.now(datetime.timezone.utc) - self.pod.metadata.creation_timestamp) > datetime.timedelta(
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
        if (datetime.datetime.now(datetime.timezone.utc) - self.pod.metadata.creation_timestamp) > datetime.timedelta(
            seconds=self.max_time_to_complete
        ):
            logger.info("Pod has timed out: %s", self.pod.metadata.name)
            return True
        return False

    def _find_start_and_end_of_pod(self, pod: V1Pod) -> Tuple[Any, Optional[Any]]:
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

    def _find_latest_raised_error(self) -> str:
        """
        For this we will make an assumption that contains "Error:" is the line that we
        want to grab and store. This is usually found at the bottom of a traceback for a
        failure. However, will default to the last line in the pod otherwise.
        :return: string containing the error found last in the logs.
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
        line_to_record: str = str(logs[-1])
        logs.reverse()
        for line in logs:
            if "Error:" in line:
                line_to_record = line
                break
        return line_to_record.strip()

    def process_job_failed(self) -> None:
        """
        Process the event that failed, and notify the message broker
        :return: None
        """
        if self.pod is None or self.job is None:
            raise AttributeError("Pod and job must be set in the JobWatcher before calling this function.")
        message = self._find_latest_raised_error()
        logger.info("Job %s has failed, with message: %s", self.job.metadata.name, message)
        reduction_id = self.job.metadata.annotations["reduction-id"]
        start, end = self._find_start_and_end_of_pod(self.pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State(State.ERROR),
            status_message=message,
            output_files=[],
            reduction_end=str(end),
            reduction_start=start,
        )

    def process_job_success(self) -> None:
        """
        Process a successful event, grab the required data and logged output that will notify the message broker
        :return: None
        """
        if self.job is None:
            raise AttributeError("Job must be set in the JobWatcher before calling this function.")
        job_name = self.job.metadata.name
        reduction_id = self.job.metadata.annotations.get("reduction-id")
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
            output = logs.split("\n")[-2]  # Get second to last line (last line is empty)
            logger.info("Job %s has been completed with output: %s", job_name, output)
            job_output = json.loads(output)
        except JSONDecodeError as exception:
            logger.error("Last message from job is not a JSON string")
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }
        except TypeError as exception:
            logger.error("Last message from job is not a string: %s", str(exception))
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }
        except Exception as exception:  # pylint:disable=broad-exception-caught
            logger.error("There was a problem recovering the job output")
            logger.exception(exception)
            job_output = {
                "status": "Unsuccessful",
                "output_files": [],
                "status_message": f"{str(exception)}",
            }

        # Grab status from output
        status = job_output.get("status", "Unsuccessful")
        status_message = job_output.get("status_message", "")
        output_files = job_output.get("output_files", [])
        start, end = self._find_start_and_end_of_pod(self.pod)
        self.db_updater.update_completed_run(
            db_reduction_id=reduction_id,
            state=State[status.upper()],
            status_message=status_message,
            output_files=output_files,
            reduction_end=str(end),
            reduction_start=start,
        )

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
