"""
Main class, creates jobs by calling to the jobcreator, creates the jobwatcher for each created job, and receives
requests from the topicconsumer.
"""

import os
import time
import uuid
from pathlib import Path
from typing import Any

from db.data_models import Job, JobType, Run, State
from db.utils.db_updater import DBUpdater

from jobcreator.job_creator import JobCreator
from jobcreator.queue_consumer import QueueConsumer
from jobcreator.script_aquisition import acquire_script
from jobcreator.utils import (
    create_ceph_mount_path_autoreduction,
    create_ceph_mount_path_simple,
    find_sha256_of_image,
    logger,
)

# Set up the jobcreator environment
DB_IP = os.environ.get("DB_IP", "")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_UPDATER = DBUpdater(ip=DB_IP, username=DB_USERNAME, password=DB_PASSWORD)

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

MAX_TIME_TO_COMPLETE = int(os.environ.get("MAX_TIME_TO_COMPLETE", 60 * 60 * 6))


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
        owner = DB_UPDATER.find_owner_db_entry_or_create(
            experiment_number=message.get("experiment_number"), user_number=message.get("user_number")
        )
        if message.get("user_number"):
            # Add UUID which will avoid collisions
            job_name = f"run-owner{str(owner.user_number).lower()}-requested-{uuid.uuid4().hex!s}"
        else:
            # Add UUID which will avoid collisions
            job_name = f"run-owner{str(owner.experiment_number).lower()}-requested-{uuid.uuid4().hex!s}"
        # Job name can be no longer than 50 characters because more will be added to end the name such as -extras-pvc
        # and is needed For defining the PVs and PVCs
        if len(job_name) > 50:  # noqa: PLR2004
            job_name = job_name[:50]
        job = Job(
            start=None,
            end=None,
            state=State.NOT_STARTED,
            inputs={},
            outputs=None,
            runner_image=runner_image,
            owner=owner,
            job_type=JobType.SIMPLE,
        )
        DB_UPDATER.add_simple_job(job=job)
        DB_UPDATER.update_script(job=job, job_script=script, script_sha="")
        ceph_mount_path_kwargs = (
            {"user_number": str(owner.user_number)}
            if owner.user_number is not None
            else {"experiment_number": str(owner.experiment_number)}
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
            job_id=job.id,
            fia_api_host=FIA_API_HOST,
            fia_api_api_key=FIA_API_API_KEY,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
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
        # Add UUID which will avoid collisions for reruns
        owner = DB_UPDATER.find_owner_db_entry_or_create(
            experiment_number=message.get("experiment_number"), user_number=message.get("user_number")
        )
        run, new_job = DB_UPDATER.add_rerun_job(
            original_job_id=int(message.get("job_id")),  # type: ignore
            new_script=script,
            new_owner_id=owner.id,
            new_runner_image=runner_image,
        )
        ceph_mount_path = create_ceph_mount_path_autoreduction(
            instrument_name=run.instrument.instrument_name,
            rb_number=str(run.owner.experiment_number) if run.owner is not None else "0",
        )
        job_name = f"run-{run.filename.lower()}-{uuid.uuid4().hex!s}"
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=str(ceph_mount_path),
            job_id=new_job.id,
            fia_api_host=FIA_API_HOST,
            fia_api_api_key=FIA_API_API_KEY,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
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
        experiment_number = message["experiment_number"]
        title = message["experiment_title"]
        users = message["users"]
        run_start = message["run_start"]
        run_end = message["run_end"]
        good_frames = message["good_frames"]
        raw_frames = message["raw_frames"]
        additional_values = message["additional_values"]
        runner_image = message.get("runner_image", DEFAULT_RUNNER)
        runner_image = find_sha256_of_image(runner_image)
        # Add UUID which will avoid collisions for reruns
        job_name = f"run-{filename.lower()}-{uuid.uuid4().hex!s}"
        job = DB_UPDATER.add_detected_run(
            instrument_name,
            Run(
                filename=filename,
                title=title,
                users=users,
                run_start=run_start,
                run_end=run_end,
                good_frames=good_frames,
                raw_frames=raw_frames,
            ),
            additional_values,
            runner_image,
            experiment_number,
        )
        job_id = job.id  # Needed due to ORM weirdness with session availability.
        script, script_sha = acquire_script(
            fia_api_host=FIA_API_HOST,
            job_id=job_id,
            instrument=instrument_name,
        )
        DB_UPDATER.update_script(job, script, script_sha)
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
    if (
        not message.get("job_id")
        and message.get("script")
        and message.get("runner_image")
        and (message.get("user_number") or message.get("experiment_number"))
    ):
        logger.info("Processing simple message...")
        process_simple_message(message)
    elif (
        message.get("job_id")
        and message.get("runner_image")
        and message.get("script")
        and (message.get("user_number") or message.get("experiment_number"))
    ):
        logger.info("Processing rerun message...")
        process_rerun_message(message)
    else:
        logger.info("Processing autoreduction message...")
        process_autoreduction_message(message)


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
