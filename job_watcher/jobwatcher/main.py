"""
Main file containing the entry point of the JobWatcher application
"""

import os

from db.utils.db_updater import DBUpdater

from jobwatcher.job_watcher import JobWatcher
from jobwatcher.utils import load_kubernetes_config

# Defaults to 6 hours
MAX_TIME_TO_COMPLETE = int(os.environ.get("MAX_TIME_TO_COMPLETE_JOB", 60 * 60 * 6))

# Grab pod info from env
CONTAINER_NAME = os.environ.get("CONTAINER_NAME", "")
JOB_NAME = os.environ.get("JOB_NAME", "")
PARTIAL_POD_NAME = os.environ.get("POD_NAME", "")

DB_IP = os.environ.get("DB_IP", "")
DB_USERNAME = os.environ.get("DB_USERNAME", "")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_UPDATER = DBUpdater(ip=DB_IP, username=DB_USERNAME, password=DB_PASSWORD)


def main() -> None:
    """
    The main entry point to the JobWatcher application
    :return:
    """
    load_kubernetes_config()
    job_watcher = JobWatcher(
        db_updater=DB_UPDATER,
        max_time_to_complete=MAX_TIME_TO_COMPLETE,
        container_name=CONTAINER_NAME,
        job_name=JOB_NAME,
        partial_pod_name=PARTIAL_POD_NAME,
    )
    job_watcher.watch()


if __name__ == "__main__":
    main()
