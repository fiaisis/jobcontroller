"""
Main class, creates jobs by calling to the jobcreator, creates the jobwatcher for each created job, and receives
requests from the topicconsumer.
"""

import os
import time
import uuid
from pathlib import Path
from typing import Any

from jobcreator.database.db_updater import DBUpdater, Run
from jobcreator.job_creator import JobCreator
from jobcreator.queue_consumer import QueueConsumer
from jobcreator.script_aquisition import acquire_script
from jobcreator.utils import create_ceph_mount_path, find_sha256_of_image, logger

from db.data_models import Job, State

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


def process_simple_message(message: dict[str, Any]):
    runner_image = find_sha256_of_image(message["runner_image"])
    script = message["script"]
    owner = DB_UPDATER.find_owner_db_entry_or_create(experiment_number=message.get("experiment_number"),
                                                     user_number=message.get("user_number"))
    script = DB_UPDATER.update_script()
    reduction = Job(
        start=None,
        end=None,
        state=State.NOT_STARTED,
        inputs={},
        script=script,
        outputs=None,
        runner_image=runner_image,
        owner_relationship=owner,
    )


def process_autoreduction_message(message: dict[str, Any]):    # pylint: disable=too-many-locals
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
        reduction = DB_UPDATER.add_detected_run(
            instrument_name,
            Run(
                filename=filename,
                title=title,
                users=users,
                experiment_number=experiment_number,
                run_start=run_start,
                run_end=run_end,
                good_frames=good_frames,
                raw_frames=raw_frames,
            ),
            additional_values,
            runner_image,
        )
        script, script_sha = acquire_script(
            fia_api_host=FIA_API_HOST,
            reduction_id=reduction.id,
            instrument=instrument_name,
        )
        DB_UPDATER.update_script(reduction, script, script_sha)
        ceph_mount_path = create_ceph_mount_path(instrument_name, rb_number)
        JOB_CREATOR.spawn_job(
            job_name=job_name,
            script=script,
            job_namespace=JOB_NAMESPACE,
            ceph_creds_k8s_secret_name=CEPH_CREDS_SECRET_NAME,
            ceph_creds_k8s_namespace=CEPH_CREDS_SECRET_NAMESPACE,
            cluster_id=CLUSTER_ID,
            fs_name=FS_NAME,
            ceph_mount_path=str(ceph_mount_path),
            reduction_id=reduction.id,
            db_ip=DB_IP,
            db_username=DB_USERNAME,
            db_password=DB_PASSWORD,
            max_time_to_complete_job=MAX_TIME_TO_COMPLETE,
            runner_image=runner_image,
            manila_share_id=MANILA_SHARE_ID,
            manila_share_access_id=MANILA_SHARE_ACCESS_ID,
        )
    except Exception as exception:  # pylint: disable=broad-exception-caught
        logger.exception(exception)


def process_message(message: dict[str, Any]) -> None:
    """
    There is an assumption that if the script and runner are provided then it is a
    simple run and we can just start and don't need to generate the script, else it's
    assumed to be an autoreduced one.
    :param message: the message is a dictionary containing the needed information for spawning a pod
    :return: None
    """
    if message.get("script") and message.get("runner_image") and (message.get("user_number") or message.get("experiment_number")):
        logger.info("Processing simple message...")
        process_simple_message(message)
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
