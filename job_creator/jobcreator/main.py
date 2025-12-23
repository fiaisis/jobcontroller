"""
Main class, creates jobs by calling to the jobcreator, creates the jobwatcher for each created job, and receives
requests from the topicconsumer.
"""

import json
import os
import time
import uuid
from pathlib import Path
from typing import Any

from jobcreator.job_creator import JobCreator
from jobcreator.queue_consumer import QueueConsumer
from jobcreator.script_acquisition import post_autoreduction_job
from jobcreator.utils import (
    create_ceph_mount_path_autoreduction,
    create_ceph_mount_path_simple,
    find_sha256_of_image,
    logger,
)

# This is used for ensuring that when on staging we will use an empty dir instead of the ceph production mount
DEV_MODE: Any = os.environ.get("DEV_MODE", "False")
DEV_MODE = DEV_MODE.lower() != "false"

if DEV_MODE:
    logger.info("Launched in dev mode")
else:
    logger.info("Launched in production mode")

DEFAULT_RUNNER_SHA: Any = os.environ.get("DEFAULT_RUNNER_SHA", None)
if DEFAULT_RUNNER_SHA is None:
    raise OSError("DEFAULT_RUNNER_SHA not set in the environment, please add it.")
DEFAULT_RUNNER = f"ghcr.io/fiaisis/mantid@sha256:{DEFAULT_RUNNER_SHA}"
IMAGING_RUNNER_SHA: Any = os.environ.get("IMAGING_RUNNER_SHA", None)
IMAGING_RUNNER = f"ghcr.io/fiaisis/mantidimaging@sha256:{IMAGING_RUNNER_SHA}"
WATCHER_SHA = os.environ.get("WATCHER_SHA", None)
if WATCHER_SHA is None:
    raise OSError("WATCHER_SHA not set in the environment, please add it.")
FIA_API_HOST = os.environ.get("FIA_API", "fia-api-service.fia.svc.cluster.local:80")
FIA_API_API_KEY = os.environ.get("FIA_API_API_KEY", "")
QUEUE_HOST = os.environ.get("QUEUE_HOST", "")
QUEUE_NAME = os.environ.get("INGRESS_QUEUE_NAME", "")
CONSUMER_USERNAME = os.environ.get("QUEUE_USER", "")
CONSUMER_PASSWORD = os.environ.get("QUEUE_PASSWORD", "")
REDUCE_USER_ID = os.environ.get("REDUCE_USER_ID", "")
JOB_NAMESPACE = os.environ.get("JOB_NAMESPACE", "fia")
JOB_CREATOR = JobCreator(dev_mode=DEV_MODE, watcher_sha=WATCHER_SHA)

CEPH_CREDS_SECRET_NAME = os.environ.get("CEPH_CREDS_SECRET_NAME", "ceph-creds")
CEPH_CREDS_SECRET_NAMESPACE = os.environ.get("CEPH_CREDS_SECRET_NAMESPACE", "fia")
CLUSTER_ID = os.environ.get("CLUSTER_ID", "ba68226a-672f-4ba5-97bc-22840318b2ec")
FS_NAME = os.environ.get("FS_NAME", "deneb")

MANILA_SHARE_ID = os.environ.get("MANILA_SHARE_ID", "05b75577-a8fb-4c87-a3f3-6a07012e80bc")
MANILA_SHARE_ACCESS_ID = os.environ.get("MANILA_SHARE_ACCESS_ID", "8045701a-0c3e-486b-a89b-4fd741d04f69")

MAX_TIME_TO_COMPLETE = int(os.environ.get("MAX_TIME_TO_COMPLETE", str(60 * 60 * 6)))


def _generate_special_pvs(instrument: str) -> list[str]:
    """
    A generic function for, based on passed args, returning what the special persistent volumes should be.
    """
    special_pvs = []

    match instrument.lower():
        case "imat":
            logger.info("Special PV for %s added.", instrument)
            special_pvs.append("imat")
        case _:
            logger.info("No special PV needed for %s", instrument)

    return special_pvs


def _select_runner_image(instrument: str) -> str:
    """
    A generic function for, based on passed args, returning what the runner that should be used.
    """
    match instrument.lower():
        case "imat":
            if IMAGING_RUNNER_SHA is not None:
                logger.info("Imaging runner image selected for %s ", instrument)
                return IMAGING_RUNNER
            logger.error("Imaging runner sha not defined in environment variables, using Default runner")
            return DEFAULT_RUNNER
        case _:
            logger.info("Using default runner image %s", instrument)
            return DEFAULT_RUNNER


def _select_taints_and_affinity(instrument: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    A generic function for, based on passed args, returning what the runner that should be used.
    """
    taints = []
    affinity = None

    match instrument.lower():
        case "imat":
            logger.info("Applying taint to the job on instrument %s", instrument)
            taints.append({"key": "nvidia.com/gpu", "effect": "NoSchedule", "operator": "Exists"})
            affinity = {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}
        case _:
            logger.info("No taints applied to %s runners", instrument)

    return taints, affinity


def process_simple_message(message: dict[str, Any]) -> None:
    """
    A simple message expects the following entries in the dictionary: (experiment_number or user_number, runner_image,
    and script).
    :param message: The message to be processed, there is an assumption it is the simple variety of message that the
    creator can process.
    :return: None
    """
    try:
        runner_image = find_sha256_of_image(message["runner_image"])
        script = message["script"]
        user_number = message.get("user_number")
        experiment_number = message.get("experiment_number")
        job_id = message.get("job_id")
        taints = message.get("taints")
        # Attempt to load from a json string list
        taints = json.loads(str(taints)) if taints is not None else []
        affinity = message.get("affinity")
        affinity = json.loads(str(taints)) if affinity is not None else {}

        if not isinstance(job_id, int):
            raise ValueError("job_id must be an integer")

        if user_number:
            # Add UUID which will avoid collisions
            job_name = f"run-owner{str(user_number).lower()}-requested-{uuid.uuid4().hex!s}"
        else:
            # Add UUID which will avoid collisions
            job_name = f"run-owner{str(experiment_number).lower()}-requested-{uuid.uuid4().hex!s}"
        # Job name can be no longer than 50 characters because more will be added to end the name such as -extras-pvc
        # and is needed For defining the PVs and PVCs
        if len(job_name) > 50:  # noqa: PLR2004
            job_name = job_name[:50]
        ceph_mount_path_kwargs = (
            {"user_number": str(user_number)} if user_number else {"experiment_number": str(experiment_number)}
        )
        ceph_mount_path = create_ceph_mount_path_simple(**ceph_mount_path_kwargs)
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=str(ceph_mount_path),
            job_id=job_id,
            fia_api_host=FIA_API_HOST,
            fia_api_api_key=FIA_API_API_KEY,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
            special_pvs=[],
            taints=taints,
            affinity=affinity,
        )
    except Exception as exception:
        logger.exception(exception)


def process_rerun_message(message: dict[str, Any]) -> None:
    """
    Rerun a reduction based on the
    :param message: dict, the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    try:
        runner_image = find_sha256_of_image(message["runner_image"])
        script = message["script"]

        ceph_mount_path = create_ceph_mount_path_autoreduction(
            instrument_name=message["instrument"],
            rb_number=str(message["rb_number"]),
        )

        special_pvs = _generate_special_pvs(instrument=message["instrument"])
        taints, affinity = _select_taints_and_affinity(instrument=message["instrument"])

        # Add UUID which will avoid collisions for reruns
        job_name = f"run-{str(message['filename']).lower()}-{uuid.uuid4().hex!s}"
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=str(ceph_mount_path),
            job_id=message["job_id"],
            fia_api_host=FIA_API_HOST,
            fia_api_api_key=FIA_API_API_KEY,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
            special_pvs=special_pvs,
            taints=taints,
            affinity=affinity,
        )
    except Exception as exception:
        logger.exception(exception)


def process_autoreduction_message(message: dict[str, Any]) -> None:
    """
    Request that the k8s api spawns a job
    :param message: dict, the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    try:
        filename = Path(message["filepath"]).stem
        rb_number = message["experiment_number"]
        instrument_name = message["instrument"]
        runner_image = message.get("runner_image")
        if runner_image is None:
            runner_image = _select_runner_image(instrument_name)
        runner_image = find_sha256_of_image(runner_image)
        autoreduction_request = {
            "filename": filename,
            "rb_number": str(rb_number),
            "instrument_name": instrument_name,
            "title": message["experiment_title"],
            "users": message["users"],
            "run_start": message["run_start"],
            "run_end": message["run_end"],
            "good_frames": int(message["good_frames"]),
            "raw_frames": int(message["raw_frames"]),
            "additional_values": message["additional_values"],
            "runner_image": runner_image,
        }

        special_pvs = _generate_special_pvs(instrument=instrument_name)
        taints, affinity = _select_taints_and_affinity(instrument=message["instrument"])

        # Add UUID which will avoid collisions for reruns
        job_name = f"run-{filename.lower()}-{uuid.uuid4().hex!s}"
        script, job_id = post_autoreduction_job(
            fia_api_host=FIA_API_HOST,
            fia_api_key=FIA_API_API_KEY,
            autoreduction_request=autoreduction_request,
        )
        ceph_mount_path = create_ceph_mount_path_autoreduction(instrument_name, rb_number)
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=str(ceph_mount_path),
            job_id=job_id,
            fia_api_host=FIA_API_HOST,
            fia_api_api_key=FIA_API_API_KEY,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
            special_pvs=special_pvs,
            taints=taints,
            affinity=affinity,
        )
    except Exception as exception:
        logger.exception(exception)


def process_message(message: dict[str, Any]) -> None:
    """
    There is an assumption that if the script and runner are provided then it is a
    simple run and we can just start and don't need to generate the script, else it's
    assumed to be an autoreduced one.
    :param message: the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    job_type = message.get("job_type", "autoreduction")
    match job_type:
        case "simple":
            logger.info("Processing simple message")
            process_simple_message(message)
        case "rerun":
            logger.info("Processing rerun message")
            process_rerun_message(message)
        case "autoreduction":
            logger.info("Processing autoreduction message")
            process_autoreduction_message(message)
        case _:
            logger.warn("message type not recognised, not starting job. Message: ", message)


def write_readiness_probe_file() -> None:
    """
    Write the file with the timestamp for the readinessprobe
    :return: None
    """
    path = Path("/tmp/heartbeat")  # noqa: S108
    with path.open("w", encoding="utf-8") as file:
        file.write(time.strftime("%Y-%m-%d %H:%M:%S"))


def main() -> None:
    """
    This is the function that runs the JobController software suite
    """
    consumer = QueueConsumer(
        process_message,
        queue_host=QUEUE_HOST,
        username=CONSUMER_USERNAME,
        password=CONSUMER_PASSWORD,
        queue_name=QUEUE_NAME,
    )
    consumer.start_consuming(write_readiness_probe_file)


if __name__ == "__main__":
    main()
