"""
A general utilities module for code that may or may not be reused throughout this repository
"""

import hashlib
import logging
import os
import sys
from pathlib import Path

import requests
from kubernetes import config  # type: ignore[import-untyped]
from kubernetes.config import ConfigException  # type: ignore[import-untyped]

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("jobcreator")


def create_ceph_path_autoreduction(instrument_name: str, rb_number: str) -> Path:
    """
    Create the path that the files should store outputs in on CEPH
    :param instrument_name: The name of the instrument that the file is from
    :param rb_number: The experiment number that the file was generated as part of
    :return: The path that the output should be in
    """
    return Path("/ceph") / instrument_name / "RBNumber" / f"RB{rb_number}" / "autoreduced"


def add_ceph_path_to_output_files(ceph_path: str, output_files: list[Path]) -> list[Path]:
    """
    Add the ceph path to the beginning of output files
    :param ceph_path: The ceph path to be appended to the front of the output files in the list
    :param output_files: The list of files output from the reduction script, that should be appended to the end of
    the ceph_path
    :return: A list with the new paths
    """
    return [Path(ceph_path).joinpath(output) for output in output_files]


def load_kubernetes_config() -> None:
    """
    Load the kubernetes config for the kubernetes library, attempt incluster first, then try the KUBECONFIG variable,
    then finally try the default kube config locations
    :return:
    """
    try:
        config.load_incluster_config()
    except ConfigException:
        # Load config that is set as KUBECONFIG in the OS or in the default location
        kubeconfig_path = os.getenv("KUBECONFIG", None)
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_kube_config()


def create_ceph_mount_path_simple(
    user_number: str | None = None,
    experiment_number: str | None = None,
    mount_path: str = "/isis/instrument",
    local_ceph_path: str = "/ceph",
) -> Path:
    """
    Creates the ceph mount for the job to output to
    :param user_number: str, The user number that owns the job
    :param experiment_number: str, The experiment number that owns the job
    :param mount_path: str, the path that should be pointed to by default, before RBNumber, and Instrument specific
    directories.
    :param local_ceph_path: str, the path that we expect Ceph to be present locally, by default it's /ceph, mostly for
    testing.
    :return: str, the path that was created for the mount
    """
    initial_path = Path(local_ceph_path) / "GENERIC" / "autoreduce"
    if user_number is not None and experiment_number is None:
        ceph_path = initial_path / "UserNumbers" / user_number
    elif experiment_number is not None and user_number is None:
        ceph_path = initial_path / "ExperimentNumbers" / experiment_number
    else:
        raise ValueError("Both user_number and experiment_number cannot be defined, but one must be.")
    if not ceph_path.exists():
        logger.info("Attempting to create ceph path: %s", str(ceph_path))
        ceph_path.mkdir(parents=True, exist_ok=True)
    # There is an assumption that the ceph_path will have /ceph at the start that needs to be removed
    ceph_path = ceph_path.relative_to(local_ceph_path)
    return Path(mount_path) / ceph_path


def ensure_ceph_path_exists_autoreduction(ceph_path: Path) -> Path:
    """
    Takes a path that is intended to be on ceph and ensures that it will be correct for what we should mount and
    apply output to.
    :param ceph_path: Is the path to where we should output to ceph
    :return: The corrected path for output to ceph path
    """
    if not ceph_path.exists():
        logger.info("Ceph path does not exist: %s", ceph_path)
        rb_folder = ceph_path.parent
        if not rb_folder.exists():
            logger.info("RBFolder (%s) does not exist, setting RBNumber folder to unknown", str(rb_folder))
            # Set parent to unknown
            rb_folder = rb_folder.with_name("unknown")
            ceph_path = rb_folder.joinpath(ceph_path.name)
        if not ceph_path.exists():
            logger.info("Attempting to create ceph path: %s", str(ceph_path))
            ceph_path.mkdir(parents=True, exist_ok=True)

    return ceph_path


def create_ceph_mount_path_autoreduction(
    instrument_name: str, rb_number: str, mount_path: str = "/isis/instrument"
) -> Path:
    """
    Creates the ceph mount for the job to output to
    :param instrument_name: str, name of the instrument
    :param rb_number: str, the rb number of the run
    :param mount_path: str, the path that should be pointed to by default, before RBNumber, and Instrument specific
    directories.
    :return: str, the path that was created for the mount
    """
    ceph_path = create_ceph_path_autoreduction(instrument_name, rb_number)
    ceph_path = ensure_ceph_path_exists_autoreduction(ceph_path)
    # There is an assumption that the ceph_path will have /ceph at the start that needs to be removed
    ceph_path = ceph_path.relative_to("/ceph")
    return Path(mount_path) / ceph_path


def get_org_image_name_and_version_from_image_path(image_path: str) -> tuple[str, str, str]:
    """
    Takes the image path and extracts just the user image parts.
    :param image_path: str, the image path to process either ghcr.io/fiaisis/mantid:6.9.1 or
    https://ghcr.io/fiaisis/mantid:6.9.1
    :return: Tuple(str, str, str), organisation name, image name, version tag in that order
    """
    image_path_without_https = image_path.split("://")[-1]
    split_image_path = image_path_without_https.split("/")
    org_name = split_image_path[1]
    image_name, version = split_image_path[2].split(":")
    return org_name, image_name, version  # Use organisation and image name without ghcr.io


def get_sha256_using_image_from_ghcr(user_image: str, version: str = "") -> str:
    """
    Take the user image and request from the github api the sha256 of the image tag
    :param user_image: str, in the format "organisation/image_name" e.g. fiaisis/mantid
    :param version: str, the tag used to refer to a specific image
    :return: str, sha256 of the image e.g. "6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec"
    """
    if ":" in version:
        version = version.split(":")[-1]

    # Get token
    token_response = requests.get(f"https://ghcr.io/token?scope=repository:{user_image}:pull", timeout=5)
    token = token_response.json().get("token")

    # Create header
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.docker.distribution.manifest.v2+json"}

    # Get response from ghcr for digest
    manifest_response = requests.get(f"https://ghcr.io/v2/{user_image}/manifests/{version}", headers=headers, timeout=5)
    manifest = manifest_response.text
    return hashlib.sha256(manifest.encode("utf-8")).hexdigest()


def find_sha256_of_image(image: str) -> str:
    """
    Return the sha256 version of the image and return the full image path.
    There is an assumption in this that the image is present on ghcr.io, if not this will fail.
    :param image: str, the image to process e.g. ghcr.io/fiaisis/mantid:6.9.1
    :return: str, Return the exact image sha256 if possible based on the image that was passed, if not possible just
    return the input. e.g.
    ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec
    """
    try:
        # If sha256 is present in image assume it is already correct.
        if "sha256:" in image:
            return image
        org_name, image_name, version = get_org_image_name_and_version_from_image_path(image)
        user_image = org_name + "/" + image_name
        logger.info("Found user image to use: %s", user_image)
        version_to_use = get_sha256_using_image_from_ghcr(user_image, version)
        logger.info("Found sha256 tag for %s: %s", user_image, version_to_use)
        return f"ghcr.io/{org_name}/{image_name}@sha256:{version_to_use}"
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning(str(e))
        return image
