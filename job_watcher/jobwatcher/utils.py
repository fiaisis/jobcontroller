"""
A general utilities module for code that may or may not be reused throughout this repository
"""

import logging
import os
import sys

from kubernetes import config  # type: ignore[import-untyped]
from kubernetes.config import ConfigException  # type: ignore[import-untyped]

stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.DEBUG,
)
logger = logging.getLogger("jobwatcher")


def load_kubernetes_config() -> None:
    """
    Load the kubernetes config for the kubernetes library, attempt in-cluster first, then try the KUBECONFIG variable,
    then finally try the default kube config locations
    :return: None
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
