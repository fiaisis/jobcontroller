import random
from unittest import mock
from unittest.mock import call

from jobcreator.job_creator import (
    JobCreator,
    _setup_ceph_pv,
    _setup_extras_pv,
    _setup_extras_pvc,
    _setup_pvc,
    _setup_smb_pv,
    _generate_tolerations_from_taints,
    _generate_affinities,
    _setup_ngem_pv_and_pvcs,
    _setup_imat_pv_and_pvcs,
)


@mock.patch("jobcreator.job_creator.client")
def test_setup_smb_pv(client):
    secret_namespace = mock.MagicMock()
    mount_options = mock.MagicMock()
    source = mock.MagicMock()
    pv_name = mock.MagicMock()
    secret_name = mock.MagicMock()

    _setup_smb_pv(
        secret_namespace=secret_namespace,
        source=source,
        mount_options=mount_options,
        pv_name=pv_name,
        secret_name=secret_name,
    )

    client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
        client.V1PersistentVolume.return_value,
    )
    client.V1PersistentVolume.assert_called_once_with(
        api_version="v1",
        kind="PersistentVolume",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1PersistentVolumeSpec.return_value,
    )
    client.V1ObjectMeta.assert_called_once_with(
        name=pv_name,
        annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"},
    )
    client.V1PersistentVolumeSpec.assert_called_once_with(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        persistent_volume_reclaim_policy="Retain",
        mount_options=mount_options,
        csi=client.V1CSIPersistentVolumeSource.return_value,
    )
    client.V1CSIPersistentVolumeSource.assert_called_once_with(
        driver="smb.csi.k8s.io",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"source": source},
        node_stage_secret_ref=client.V1SecretReference.return_value,
    )
    client.V1SecretReference.assert_called_once_with(name=secret_name, namespace=secret_namespace)


@mock.patch("jobcreator.job_creator.client")
def test_setup_pvc(client):
    pvc_name = mock.MagicMock()
    pv_name = mock.MagicMock()
    namespace = mock.MagicMock()
    access_mode = mock.MagicMock()

    _setup_pvc(pvc_name, pv_name, namespace, access_mode)

    client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
    client.V1ResourceRequirements.assert_called_once_with(requests={"storage": "1000Gi"})
    client.V1PersistentVolumeClaimSpec.assert_called_once_with(
        access_modes=[access_mode],
        resources=client.V1ResourceRequirements.return_value,
        volume_name=pv_name,
        storage_class_name="",
    )
    client.V1PersistentVolumeClaim.assert_called_once_with(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1PersistentVolumeClaimSpec.return_value,
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim.assert_called_once_with(
        namespace=namespace, body=client.V1PersistentVolumeClaim.return_value
    )


@mock.patch("jobcreator.job_creator.client")
def test_setup_extras_pvc(client):
    job_name = str(mock.MagicMock())
    job_namespace = str(mock.MagicMock())
    pvc_name = f"{job_name}-extras-pvc"
    pv_name = mock.MagicMock()

    assert _setup_extras_pvc(job_name, job_namespace, pv_name) == pvc_name

    client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
    client.V1ResourceRequirements(requests={"storage": "1000Gi"})
    client.V1LabelSelectorRequirement.assert_called_once_with(key="name", operator="In", values=[pv_name])
    client.V1LabelSelector.assert_called_once_with(
        match_expressions=[client.V1LabelSelectorRequirement.return_value],
    )
    client.V1PersistentVolumeClaimSpec.assert_called_once_with(
        access_modes=["ReadOnlyMany"],
        resources=client.V1ResourceRequirements.return_value,
        selector=client.V1LabelSelector.return_value,
        storage_class_name="",
    )
    client.V1PersistentVolumeClaim.assert_called_once_with(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1PersistentVolumeClaimSpec.return_value,
    )
    client.CoreV1Api.return_value.create_namespaced_persistent_volume_claim.assert_called_once_with(
        namespace=job_namespace,
        body=client.V1PersistentVolumeClaim.return_value,
    )


@mock.patch("jobcreator.job_creator.client")
def test_setup_extras_pv(client):
    job_name = str(mock.MagicMock())
    pv_name = f"{job_name}-extras-pv"
    secret_namespace = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()

    assert _setup_extras_pv(job_name, secret_namespace, manila_share_id, manila_share_access_id) == pv_name

    client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
        client.V1PersistentVolume.return_value,
    )
    client.V1PersistentVolume.assert_called_once_with(
        api_version="v1",
        kind="PersistentVolume",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1PersistentVolumeSpec.return_value,
    )
    client.V1ObjectMeta.assert_called_once_with(name=pv_name, labels={"name": pv_name})
    client.V1PersistentVolumeSpec.assert_called_once_with(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        csi=client.V1CSIPersistentVolumeSource.return_value,
    )
    client.V1CSIPersistentVolumeSource.assert_called_once_with(
        driver="cephfs.manila.csi.openstack.org",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"shareID": manila_share_id, "shareAccessID": manila_share_access_id},
        node_stage_secret_ref=client.V1SecretReference.return_value,
        node_publish_secret_ref=client.V1SecretReference.return_value,
    )
    client.V1SecretReference.assert_called_once_with(name="manila-creds", namespace=secret_namespace)


@mock.patch("jobcreator.job_creator.client")
def test_generate_tolerations_from_taints(client):
    taints = [
        {"key": "key1", "value": "value1", "operator": "Equal", "effect": "NoSchedule"},
        {"key": "key2", "operator": "Exists", "effect": "NoExecute"},
    ]
    tolerations = _generate_tolerations_from_taints(taints)

    assert len(tolerations) == 2
    client.V1Toleration.assert_has_calls(
        [
            call(key="key1", value="value1", operator="Equal", effect="NoSchedule"),
            call(key="key2", value=None, operator="Exists", effect="NoExecute"),
        ]
    )


@mock.patch("jobcreator.job_creator.client")
def test_generate_affinities_none(client):
    affinity = _generate_affinities(None)
    assert affinity == client.V1Affinity.return_value
    client.V1Affinity.assert_called_once_with(pod_anti_affinity=client.V1PodAntiAffinity.return_value)


@mock.patch("jobcreator.job_creator.logger")
@mock.patch("jobcreator.job_creator.client")
def test_generate_affinities_missing_key(client, logger):
    node_affinity_dict = {"key": "some-key", "operator": "In"}  # missing "values"
    affinity = _generate_affinities(node_affinity_dict)
    logger.error.assert_called_once()
    client.V1Affinity.assert_called_once_with(pod_anti_affinity=client.V1PodAntiAffinity.return_value)


@mock.patch("jobcreator.job_creator.client")
def test_generate_affinities_valid(client):
    node_affinity_dict = {"key": "some-key", "operator": "In", "values": ["val1"]}
    affinity = _generate_affinities(node_affinity_dict)
    client.V1Affinity.assert_called_once_with(
        pod_anti_affinity=client.V1PodAntiAffinity.return_value, node_affinity=client.V1NodeAffinity.return_value
    )


@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
def test_setup_ngem_pv_and_pvcs(setup_smb_pv, setup_pvc):
    pv_names = []
    pvc_names = []
    _setup_ngem_pv_and_pvcs("job1", "ns1", pv_names, pvc_names)
    setup_smb_pv.assert_called_once_with(
        "job1-ngem-pv-smb", "archive-creds", "ns1", "//isis.cclrc.ac.uk/Science", [], "ReadWriteMany"
    )
    setup_pvc.assert_called_once_with("job1-ngem-pvc", "job1-ngem-pv-smb", "ns1", "ReadWriteMany")
    assert pv_names == ["job1-ngem-pv-smb"]
    assert pvc_names == ["job1-ngem-pvc"]


@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
def test_setup_imat_pv_and_pvcs(setup_smb_pv, setup_pvc):
    pv_names = []
    pvc_names = []
    _setup_imat_pv_and_pvcs("job1", "ns1", pv_names, pvc_names)
    setup_smb_pv.assert_called_once_with(
        "job1-ndximat-pv-smb",
        "imat-creds",
        "ns1",
        "//NDXIMAT.isis.cclrc.ac.uk/data$/",
        [],
    )
    setup_pvc.assert_called_once_with("job1-ndximat-pvc", "job1-ndximat-pv-smb", "ns1")
    assert pv_names == ["job1-ndximat-pv-smb"]
    assert pvc_names == ["job1-ndximat-pvc"]


@mock.patch("jobcreator.job_creator.client")
def test_setup_ceph_pv(client):
    pv_name = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()

    assert (
        _setup_ceph_pv(
            pv_name=pv_name,
            ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
            ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
            cluster_id=cluster_id,
            fs_name=fs_name,
            ceph_mount_path=ceph_mount_path,
        )
        == pv_name
    )

    client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
        client.V1PersistentVolume.return_value,
    )
    client.V1PersistentVolume.assert_called_once_with(
        api_version="v1",
        kind="PersistentVolume",
        metadata=client.V1ObjectMeta.return_value,
        spec=client.V1PersistentVolumeSpec.return_value,
    )
    client.V1ObjectMeta.assert_called_once_with(name=pv_name)
    client.V1PersistentVolumeSpec.assert_called_once_with(
        capacity={"storage": "1000Gi"},
        storage_class_name="",
        access_modes=["ReadWriteMany"],
        persistent_volume_reclaim_policy="Retain",
        volume_mode="Filesystem",
        csi=client.V1CSIPersistentVolumeSource.return_value,
    )
    client.V1CSIPersistentVolumeSource.assert_called_once_with(
        driver="cephfs.csi.ceph.com",
        node_stage_secret_ref=client.V1SecretReference.return_value,
        volume_handle=pv_name,
        volume_attributes={
            "clusterID": cluster_id,
            "mounter": "fuse",
            "fsName": fs_name,
            "staticVolume": "true",
            "rootPath": ceph_mount_path,
        },
    )
    client.V1SecretReference.assert_called_once_with(
        name=ceph_creds_k8s_secret_name,
        namespace=ceph_creds_k8s_namespace,
    )


@mock.patch("jobcreator.job_creator.load_kubernetes_config")
def test_jobcreator_init(mock_load_kubernetes_config):
    JobCreator("", False)

    mock_load_kubernetes_config.assert_called_once()


@mock.patch("jobcreator.job_creator._setup_ngem_pv_and_pvcs")
@mock.patch("jobcreator.job_creator._setup_imat_pv_and_pvcs")
@mock.patch("jobcreator.job_creator._setup_extras_pv")
@mock.patch("jobcreator.job_creator._setup_extras_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_ceph_pv")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_ngem(
    client,
    _,  # noqa: PT019
    setup_ceph_pv,
    setup_pvc,
    setup_smb_pv,
    setup_extras_pvc,
    setup_extras_pv,
    setup_imat_pv,
    setup_ngem_pv,
):
    job_name = "test-job"
    script = "test-script"
    job_namespace = "test-ns"
    watcher_sha = "test-sha"
    job_creator = JobCreator(watcher_sha, False)

    job_creator.spawn_job(
        job_name=job_name,
        script=script,
        job_namespace=job_namespace,
        ceph_creds_k8s_secret_name="secret",
        ceph_creds_k8s_namespace="ns",
        cluster_id="id",
        fs_name="fs",
        ceph_mount_path="/path",
        job_id=1,
        max_time_to_complete_job=100,
        fia_api_host="host",
        fia_api_api_key="key",
        runner_image="image",
        manila_share_id="mid",
        manila_share_access_id="maid",
        special_pvs=["ngem"],
        taints=[],
        affinity=None,
    )

    setup_ngem_pv.assert_called_once()
    # Check that ngem volume and volume mount were added
    # We check if V1Volume was called with name="ngem-mount"
    assert any(c.kwargs.get('name') == "ngem-mount" for c in client.V1Volume.call_args_list)
    # Check if V1VolumeMount was called with name="ngem-mount"
    assert any(c.kwargs.get('name') == "ngem-mount" for c in client.V1VolumeMount.call_args_list)


@mock.patch("jobcreator.job_creator._setup_extras_pv")
@mock.patch("jobcreator.job_creator._setup_extras_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_ceph_pv")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_true(
    client,
    _,  # noqa: PT019
    setup_ceph_pv,
    setup_pvc,
    setup_smb_pv,
    setup_extras_pvc,
    setup_extras_pv,
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
        volumes=[client.V1Volume.return_value, client.V1Volume.return_value, client.V1Volume.return_value],
    )
    assert (
        call(name="ceph-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-ceph-pvc", read_only=False)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="extras-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-extras-pvc", read_only=True)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="archive-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-archive-pvc", read_only=True)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert client.V1Volume.call_count == 3  # noqa: PLR2004
    assert client.V1PersistentVolumeClaimVolumeSource.call_count == 3  # noqa: PLR2004
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
            volume_mounts=[
                client.V1VolumeMount(name="archive-mount", mount_path="/archive"),
                client.V1VolumeMount(name="ceph-mount", mount_path="/output"),
                client.V1VolumeMount(name="extras-mount", mount_path="/extras"),
            ],
        )
        in client.V1Container.call_args_list
    )
    assert client.V1Container.call_count == 2  # noqa: PLR2004
    setup_ceph_pv.assert_called_once_with(
        str(job_name) + "-ceph-pv",
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
    )
    assert setup_pvc.call_count == 3  # noqa: PLR2004


@mock.patch("jobcreator.job_creator._setup_extras_pv")
@mock.patch("jobcreator.job_creator._setup_extras_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_ceph_pv")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_true_imat(
    client,
    _,  # noqa: PT019
    setup_ceph_pv,
    setup_smb_pv,
    setup_pvc,
    setup_extras_pvc,
    setup_extras_pv,
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
        tolerations=[client.V1Toleration.return_value],
        volumes=[
            client.V1Volume.return_value,
            client.V1Volume.return_value,
            client.V1Volume.return_value,
            client.V1Volume.return_value,
            client.V1Volume.return_value,
        ],
    )
    assert (
        call(name="ceph-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-ceph-pvc", read_only=False)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="extras-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-extras-pvc", read_only=True)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="archive-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-archive-pvc", read_only=True)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="imat-mount", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value)
        in client.V1Volume.call_args_list
    )
    assert (
        call(claim_name=f"{job_name}-ndximat-pvc", read_only=True)
        in client.V1PersistentVolumeClaimVolumeSource.call_args_list
    )
    assert (
        call(name="dev-shm", empty_dir=client.V1EmptyDirVolumeSource(size_limit="32Gi", medium="Memory"))
        in client.V1Volume.call_args_list
    )
    assert call(name="dev-shm", mount_path="/dev/shm") in client.V1VolumeMount.call_args_list  # noqa: S108
    assert client.V1Volume.call_count == 5  # noqa: PLR2004
    assert client.V1PersistentVolumeClaimVolumeSource.call_count == 4  # noqa: PLR2004
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
            volume_mounts=[
                client.V1VolumeMount(name="archive-mount", mount_path="/archive"),
                client.V1VolumeMount(name="ceph-mount", mount_path="/output"),
                client.V1VolumeMount(name="extras-mount", mount_path="/extras"),
                client.V1VolumeMount(name="imat-mount", mount_path="/imat"),
                client.V1VolumeMount(name="dev-shm", mount_path="/dev/shm"),  # noqa: S108
            ],
        )
        in client.V1Container.call_args_list
    )
    assert client.V1Container.call_count == 2  # noqa: PLR2004
    setup_ceph_pv.assert_called_once_with(
        str(job_name) + "-ceph-pv",
        ceph_creds_k8s_secret_name,
        ceph_creds_k8s_namespace,
        cluster_id,
        fs_name,
        ceph_mount_path,
    )


@mock.patch("jobcreator.job_creator._setup_extras_pv")
@mock.patch("jobcreator.job_creator._setup_extras_pvc")
@mock.patch("jobcreator.job_creator._setup_smb_pv")
@mock.patch("jobcreator.job_creator._setup_pvc")
@mock.patch("jobcreator.job_creator._setup_ceph_pv")
@mock.patch("jobcreator.job_creator.load_kubernetes_config")
@mock.patch("jobcreator.job_creator.client")
def test_jobcreator_spawn_job_dev_mode_false(
    client,
    _,  # noqa: PT019
    setup_ceph_pv,
    setup_smb_pv,
    setup_pvc,
    setup_extras_pvc,
    setup_extras_pv,
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

    assert (
        call(name="ceph-mount", empty_dir=client.V1EmptyDirVolumeSource.return_value) in client.V1Volume.call_args_list
    )
    client.V1EmptyDirVolumeSource.assert_called_once_with(size_limit="100Gi")
    assert client.V1Volume.call_count == 3  # noqa: PLR2004
    assert client.V1PersistentVolumeClaimVolumeSource.call_count == 2  # noqa: PLR2004

    setup_ceph_pv.assert_not_called()
