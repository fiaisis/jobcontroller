import os
import random
from pathlib import Path
from unittest import mock

import pytest
from kubernetes.config import ConfigException

from jobcreator.utils import (
    create_ceph_mount_path_simple,
    ensure_ceph_path_exists_autoreduction,
    find_sha256_of_image,
    get_org_image_name_and_version_from_image_path,
    get_sha256_using_image_from_ghcr,
    load_kubernetes_config,
)


@mock.patch("jobcreator.utils.config")
def test_config_grabbed_from_incluster(kubernetes_config):
    load_kubernetes_config()

    kubernetes_config.load_incluster_config.assert_called_once_with()


@mock.patch("jobcreator.utils.config")
def test_not_in_cluster_grab_kubeconfig_from_env_var(kubernetes_config):
    kubeconfig_path = mock.MagicMock()
    kubernetes_config.load_incluster_config = mock.MagicMock(side_effect=ConfigException)
    os.environ["KUBECONFIG"] = str(kubeconfig_path)

    load_kubernetes_config()

    kubernetes_config.load_incluster_config.assert_called_once_with()
    kubernetes_config.load_kube_config.assert_called_once_with(config_file=str(kubeconfig_path))
    os.environ.pop("KUBECONFIG", None)


@mock.patch("jobcreator.utils.config")
def test_not_in_cluster_and_not_in_env_grab_kubeconfig_from_default_location(kubernetes_config):
    os.environ.pop("KUBECONFIG", None)
    kubernetes_config.load_incluster_config = mock.MagicMock(side_effect=ConfigException)

    load_kubernetes_config()

    kubernetes_config.load_incluster_config.assert_called_once_with()
    kubernetes_config.load_kube_config.assert_called_once_with()


def test_ensure_ceph_path_exists():
    initial_path = Path("/tmp/ceph/mari/RBNumber/RB99999999/autoreduced/")  # noqa: S108

    end_path = ensure_ceph_path_exists_autoreduction(initial_path)

    assert end_path == Path("/tmp/ceph/mari/RBNumber/unknown/autoreduced")  # noqa: S108
    os.removedirs("/tmp/ceph/mari/RBNumber/unknown/autoreduced")  # noqa: S108


@pytest.mark.parametrize(("version", "expected_version"), [("6.9.1", "6.9.1"), (":6.9.1", "6.9.1")])
@mock.patch("jobcreator.utils.requests")
def test_get_sha256_using_image_from_ghcr(requests, version, expected_version):
    user_image = "fiaisis/mantid"
    response = mock.MagicMock()
    response.text = "requests_response"
    requests.get.return_value = response
    expected_headers = {
        "Authorization": f"Bearer {response.json.return_value.get.return_value}",
        "Accept": "application/vnd.docker.distribution.manifest.v2+json",
    }

    get_sha256_using_image_from_ghcr(user_image, version)

    assert requests.get.call_count == 2  # noqa: PLR2004
    assert requests.get.call_args_list[0] == mock.call(
        f"https://ghcr.io/token?scope=repository:{user_image}:pull",
        timeout=5,
    )
    assert requests.get.call_args_list[1] == mock.call(
        f"https://ghcr.io/v2/{user_image}/manifests/{expected_version}",
        timeout=5,
        headers=expected_headers,
    )


@mock.patch("jobcreator.utils.logger")
@mock.patch("jobcreator.utils.get_sha256_using_image_from_ghcr")
@mock.patch("jobcreator.utils.get_org_image_name_and_version_from_image_path", side_effect=Exception)
def test_find_sha256_of_image_exception_is_raised(_, __, logger):  # noqa: PT019
    image = str(mock.MagicMock())

    return_value = find_sha256_of_image(image)

    logger.warning.assert_called_once_with(str(Exception("")))
    assert image == return_value


def test_find_sha256_of_image_sha256_in_image():
    input_value = "ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec"

    return_value = find_sha256_of_image(input_value)

    assert return_value == input_value


@mock.patch(
    "jobcreator.utils.get_sha256_using_image_from_ghcr",
    return_value="6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec",
)
@mock.patch(
    "jobcreator.utils.get_org_image_name_and_version_from_image_path",
    return_value=("fiaisis", "mantid", "6.9.1"),
)
def test_find_sha256_of_image_just_version(_, __):  # noqa: PT019
    image_path = "https://ghcr.io/fiaisis/mantid:6.9.1"

    return_value = find_sha256_of_image(image_path)

    assert (
        return_value == "ghcr.io/fiaisis/mantid@sha256:6e5f2d070bb67742f354948d68f837a740874d230714eaa476d35ab6ad56caec"
    )


@pytest.mark.parametrize("https", ["https://", ""])
def test_get_org_image_name_and_version_from_image_path(https):
    image_path = f"{https}ghcr.io/fiaisis/mantid:6.9.1"

    org_name, image_name, version = get_org_image_name_and_version_from_image_path(image_path)

    assert org_name == "fiaisis"
    assert image_name == "mantid"
    assert version == "6.9.1"


def test_create_ceph_mount_path_simple_user_number():
    user_number = random.randint(0, 999999)  # noqa: S311

    return_value = create_ceph_mount_path_simple(
        user_number=str(user_number), local_ceph_path=Path.cwd() / "test_files"
    )

    assert return_value == Path(f"/isis/instrument/GENERIC/autoreduce/UserNumbers/{user_number}")


def test_create_ceph_mount_path_simple_experiment_number():
    experiment_number = random.randint(0, 999999)  # noqa: S311

    return_value = create_ceph_mount_path_simple(
        experiment_number=str(experiment_number), local_ceph_path=Path.cwd() / "test_files"
    )

    assert return_value == Path(f"/isis/instrument/GENERIC/autoreduce/ExperimentNumbers/{experiment_number}")


def test_create_ceph_mount_path_simple_both():
    experiment_number = random.randint(0, 999999)  # noqa: S311
    user_number = random.randint(0, 999999)  # noqa: S311

    with pytest.raises(ValueError):  # noqa: PT011
        create_ceph_mount_path_simple(
            user_number=str(user_number),
            experiment_number=str(experiment_number),
            local_ceph_path=Path.cwd() / "test_files",
        )


def test_create_ceph_mount_path_simple_user_number_with_defined_mount_path():
    user_number = random.randint(0, 999999)  # noqa: S311

    return_value = create_ceph_mount_path_simple(
        user_number=str(user_number), mount_path="/crazy/mount/path", local_ceph_path=Path.cwd() / "test_files"
    )

    assert return_value == Path(f"/crazy/mount/path/GENERIC/autoreduce/UserNumbers/{user_number}")
