import random
from unittest import mock
from unittest.mock import call

from jobcreator.job_creator import JobCreator
from jobcreator.storage import VolumeBundle


@mock.patch("jobcreator.job_creator.load_kubernetes_config")
def test_jobcreator_init(mock_load_kubernetes_config):
    JobCreator("", False)

    mock_load_kubernetes_config.assert_called_once()


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_true(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = mock.MagicMock()
    script = mock.MagicMock()
    job_namespace = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()
    reduction_id = random.randint(1, 100)  # noqa: S311
    max_time_to_complete_job = random.randint(1, 20000)  # noqa: S311
    fia_api_host = mock.MagicMock()
    fia_api_api_key = mock.MagicMock()
    watcher_sha = mock.MagicMock()
    job_creator = JobCreator(watcher_sha, False)
    runner_image = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()
    special_pvs = mock.MagicMock()
    taints = mock.MagicMock()
    affinity = {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}

    mock_volume = mock.MagicMock()
    mock_mount = mock.MagicMock()
    mock_build_volumes.return_value = VolumeBundle(
        volumes=[mock_volume],
        volume_mounts=[mock_mount],
        pv_names=["pv-1", "pv-2"],
        pvc_names=["pvc-1", "pvc-2"],
    )

    job_creator.spawn_job(
        job_name,
        script,
        job_namespace,
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
        reduction_id,
        max_time_to_complete_job,
        fia_api_host,
        fia_api_api_key,
        runner_image,
        manila_share_id,
        manila_share_access_id,
        special_pvs,
        taints,
        affinity,
    )

    mock_build_volumes.assert_called_once_with(
        job_name=job_name,
        job_namespace=job_namespace,
        dev_mode=False,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        special_pvs=special_pvs,
        ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
        cluster_id=cluster_id,
        fs_name=fs_name,
        ceph_mount_path=ceph_mount_path,
    )

    assert client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["namespace"] == job_namespace
    assert client.BatchV1Api.return_value.create_namespaced_job.call_args.kwargs["body"] == client.V1Job.return_value
    client.V1Job.assert_called_once_with(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1JobSpec.return_value,
    )
    assert (
        call(labels={"reduce.isis.cclrc.ac.uk/job-source": "automated-reduction"}) in client.V1ObjectMeta.call_args_list
    )
    assert client.V1ObjectMeta.call_count == 2  # noqa: PLR2004
    client.V1JobSpec.assert_called_once_with(
        template=client.V1PodTemplateSpec.return_value,
        backoff_limit=0,
        ttl_seconds_after_finished=21600,
    )
    client.V1PodTemplateSpec.assert_called_once_with(
        spec=client.V1PodSpec.return_value, metadata=client.V1ObjectMeta.return_value
    )
    client.V1LabelSelector.assert_called_once_with(
        match_labels={"reduce.isis.cclrc.ac.uk/job-source": "automated-reduction"},
    )
    client.V1PodAffinityTerm.assert_called_once_with(
        topology_key="kubernetes.io/hostname",
        label_selector=client.V1LabelSelector.return_value,
    )
    client.V1WeightedPodAffinityTerm.assert_called_once_with(
        weight=100,
        pod_affinity_term=client.V1PodAffinityTerm.return_value,
    )
    client.V1PodAntiAffinity.assert_called_once_with(
        preferred_during_scheduling_ignored_during_execution=[client.V1WeightedPodAffinityTerm.return_value],
    )
    client.V1NodeAffinity.assert_called_once_with(
        required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
            node_selector_terms=[
                client.V1NodeSelectorTerm(
                    match_expressions=[
                        client.V1NodeSelectorRequirement(key="node-type", operator="In", values=["gpu-worker"])
                    ]
                )
            ]
        )
    )
    client.V1Affinity.assert_called_once_with(
        pod_anti_affinity=client.V1PodAntiAffinity.return_value, node_affinity=client.V1NodeAffinity.return_value
    )
    client.V1PodSpec.assert_called_once_with(
        affinity=client.V1Affinity.return_value,
        service_account_name="jobwatcher",
        containers=[client.V1Container.return_value, client.V1Container.return_value],
        restart_policy="Never",
        tolerations=[],
        volumes=[mock_volume],
        runtime_class_name=None,
    )
    assert (
        call(
            name="job-watcher",
            image=f"ghcr.io/fiaisis/jobwatcher@sha256:{watcher_sha}",
            env=[
                client.V1EnvVar(name="FIA_API_HOST", value=fia_api_host),
                client.V1EnvVar(name="FIA_API_API_KEY", value=fia_api_api_key),
                client.V1EnvVar(name="MAX_TIME_TO_COMPLETE_JOB", value=str(max_time_to_complete_job)),
                client.V1EnvVar(name="CONTAINER_NAME", value=job_name),
                client.V1EnvVar(name="JOB_NAME", value=job_name),
                client.V1EnvVar(name="POD_NAME", value=job_name),
            ],
        )
        in client.V1Container.call_args_list
    )
    assert (
        call(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=[mock_mount],
            resources=None,
        )
        in client.V1Container.call_args_list
    )
    assert client.V1Container.call_count == 2  # noqa: PLR2004


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_true_imat(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = mock.MagicMock()
    script = mock.MagicMock()
    job_namespace = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()
    reduction_id = random.randint(1, 100)  # noqa: S311
    max_time_to_complete_job = random.randint(1, 20000)  # noqa: S311
    fia_api_host = mock.MagicMock()
    fia_api_api_key = mock.MagicMock()
    watcher_sha = mock.MagicMock()
    job_creator = JobCreator(watcher_sha, False)
    runner_image = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()
    special_pvs = ["imat"]
    taints = [{"key": "nvidia.com/gpu", "effect": "NoSchedule", "operator": "Exists"}]
    affinity = {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}

    mock_volume1 = mock.MagicMock()
    mock_volume2 = mock.MagicMock()
    mock_mount1 = mock.MagicMock()
    mock_mount2 = mock.MagicMock()
    mock_build_volumes.return_value = VolumeBundle(
        volumes=[mock_volume1, mock_volume2],
        volume_mounts=[mock_mount1, mock_mount2],
        pv_names=["pv-imat"],
        pvc_names=["pvc-imat"],
    )

    job_creator.spawn_job(
        job_name,
        script,
        job_namespace,
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
        reduction_id,
        max_time_to_complete_job,
        fia_api_host,
        fia_api_api_key,
        runner_image,
        manila_share_id,
        manila_share_access_id,
        special_pvs,
        taints,
        affinity,
    )

    mock_build_volumes.assert_called_once_with(
        job_name=job_name,
        job_namespace=job_namespace,
        dev_mode=False,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        special_pvs=["imat"],
        ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
        cluster_id=cluster_id,
        fs_name=fs_name,
        ceph_mount_path=ceph_mount_path,
    )

    client.V1PodSpec.assert_called_once_with(
        affinity=client.V1Affinity.return_value,
        service_account_name="jobwatcher",
        containers=[client.V1Container.return_value, client.V1Container.return_value],
        restart_policy="Never",
        tolerations=[
            client.V1Toleration(
                value=None,
                key="nvidia.com/gpu",
                operator="Exists",
                effect="NoSchedule",
            )
        ],
        volumes=[mock_volume1, mock_volume2],
        runtime_class_name="nvidia",
    )
    assert (
        call(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=[mock_mount1, mock_mount2],
            resources=client.V1ResourceRequirements(limits={"nvidia.com/gpu": "1"}),
        )
        in client.V1Container.call_args_list
    )


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_true_gem(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = mock.MagicMock()
    script = mock.MagicMock()
    job_namespace = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()
    reduction_id = random.randint(1, 100)  # noqa: S311
    max_time_to_complete_job = random.randint(1, 20000)  # noqa: S311
    fia_api_host = mock.MagicMock()
    fia_api_api_key = mock.MagicMock()
    watcher_sha = mock.MagicMock()
    job_creator = JobCreator(watcher_sha, False)
    runner_image = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()
    special_pvs = ["gem"]
    taints = []
    affinity = {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}

    mock_volume = mock.MagicMock()
    mock_mount = mock.MagicMock()
    mock_build_volumes.return_value = VolumeBundle(
        volumes=[mock_volume],
        volume_mounts=[mock_mount],
        pv_names=["pv-gem"],
        pvc_names=["pvc-gem"],
    )

    job_creator.spawn_job(
        job_name,
        script,
        job_namespace,
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
        reduction_id,
        max_time_to_complete_job,
        fia_api_host,
        fia_api_api_key,
        runner_image,
        manila_share_id,
        manila_share_access_id,
        special_pvs,
        taints,
        affinity,
    )

    mock_build_volumes.assert_called_once_with(
        job_name=job_name,
        job_namespace=job_namespace,
        dev_mode=False,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        special_pvs=["gem"],
        ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
        cluster_id=cluster_id,
        fs_name=fs_name,
        ceph_mount_path=ceph_mount_path,
    )

    client.V1PodSpec.assert_called_once_with(
        affinity=client.V1Affinity.return_value,
        service_account_name="jobwatcher",
        containers=[client.V1Container.return_value, client.V1Container.return_value],
        restart_policy="Never",
        tolerations=[],
        volumes=[mock_volume],
        runtime_class_name=None,
    )
    assert (
        call(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=[mock_mount],
            resources=None,
        )
        in client.V1Container.call_args_list
    )


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_false(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = mock.MagicMock()
    script = mock.MagicMock()
    job_namespace = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()
    reduction_id = random.randint(1, 100)  # noqa: S311
    max_time_to_complete_job = random.randint(1, 20000)  # noqa: S311
    fia_api_host = mock.MagicMock()
    fia_api_api_key = mock.MagicMock()
    runner_sha = mock.MagicMock()
    job_creator = JobCreator(mock.MagicMock(), True)
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()
    special_pvs = mock.MagicMock()
    taints = mock.MagicMock()
    affinity = mock.MagicMock()

    mock_volume = mock.MagicMock()
    mock_mount = mock.MagicMock()
    mock_build_volumes.return_value = VolumeBundle(
        volumes=[mock_volume],
        volume_mounts=[mock_mount],
        pv_names=[],
        pvc_names=[],
    )

    job_creator.spawn_job(
        job_name,
        script,
        job_namespace,
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
        reduction_id,
        max_time_to_complete_job,
        fia_api_host,
        fia_api_api_key,
        runner_sha,
        manila_share_id,
        manila_share_access_id,
        special_pvs,
        taints,
        affinity,
    )

    mock_build_volumes.assert_called_once_with(
        job_name=job_name,
        job_namespace=job_namespace,
        dev_mode=True,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        special_pvs=special_pvs,
        ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
        cluster_id=cluster_id,
        fs_name=fs_name,
        ceph_mount_path=ceph_mount_path,
    )


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_with_storage_bundle(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = "test-job"
    script = "echo hello"
    job_namespace = "fia"
    job_id = 42
    max_time_to_complete_job = 3600
    fia_api_host = "http://fia-api"
    fia_api_api_key = "secret-key"
    watcher_sha = "sha-watcher"
    runner_image = "mantid-image"
    job_creator = JobCreator(watcher_sha, False)

    mock_volume = mock.MagicMock()
    mock_mount = mock.MagicMock()
    storage_bundle = VolumeBundle(
        volumes=[mock_volume],
        volume_mounts=[mock_mount],
        pv_names=["bundle-pv"],
        pvc_names=["bundle-pvc"],
    )

    job_creator.spawn_job(
        job_name=job_name,
        script=script,
        job_namespace=job_namespace,
        job_id=job_id,
        max_time_to_complete_job=max_time_to_complete_job,
        fia_api_host=fia_api_host,
        fia_api_api_key=fia_api_api_key,
        runner_image=runner_image,
        storage_bundle=storage_bundle,
        gpu_job=False,
    )

    mock_build_volumes.assert_not_called()

    client.V1PodSpec.assert_called_once_with(
        affinity=client.V1Affinity.return_value,
        service_account_name="jobwatcher",
        containers=[client.V1Container.return_value, client.V1Container.return_value],
        restart_policy="Never",
        tolerations=[],
        volumes=[mock_volume],
        runtime_class_name=None,
    )
    assert (
        call(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=[mock_mount],
            resources=None,
        )
        in client.V1Container.call_args_list
    )
    assert (
        call(
            name=job_name,
            annotations={
                "job-id": str(job_id),
                "pvs": "['bundle-pv']",
                "pvcs": "['bundle-pvc']",
                "kubectl.kubernetes.io/default-container": client.V1Container.return_value.name,
            },
        )
        in client.V1ObjectMeta.call_args_list
    )


@mock.patch("jobcreator.job_creator.build_job_volumes")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_with_storage_bundle_gpu(
    client,
    _,  # noqa: PT019
    mock_build_volumes,
):
    job_name = "test-gpu-job"
    script = "echo gpu"
    job_namespace = "fia"
    job_id = 99
    max_time_to_complete_job = 7200
    fia_api_host = "http://fia-api"
    fia_api_api_key = "secret-key"
    watcher_sha = "sha-watcher"
    runner_image = "mantidimaging-image"
    job_creator = JobCreator(watcher_sha, False)

    mock_volume = mock.MagicMock()
    mock_mount = mock.MagicMock()
    storage_bundle = VolumeBundle(
        volumes=[mock_volume],
        volume_mounts=[mock_mount],
        pv_names=["gpu-pv"],
        pvc_names=["gpu-pvc"],
    )

    job_creator.spawn_job(
        job_name=job_name,
        script=script,
        job_namespace=job_namespace,
        job_id=job_id,
        max_time_to_complete_job=max_time_to_complete_job,
        fia_api_host=fia_api_host,
        fia_api_api_key=fia_api_api_key,
        runner_image=runner_image,
        storage_bundle=storage_bundle,
        gpu_job=True,
    )

    mock_build_volumes.assert_not_called()

    client.V1PodSpec.assert_called_once_with(
        affinity=client.V1Affinity.return_value,
        service_account_name="jobwatcher",
        containers=[client.V1Container.return_value, client.V1Container.return_value],
        restart_policy="Never",
        tolerations=[],
        volumes=[mock_volume],
        runtime_class_name="nvidia",
    )
    assert (
        call(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=[mock_mount],
            resources=client.V1ResourceRequirements(limits={"nvidia.com/gpu": "1"}),
        )
        in client.V1Container.call_args_list
    )
