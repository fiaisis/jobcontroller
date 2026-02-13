"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by message to the RunMaker
"""

from typing import Any

from kubernetes import client  # type: ignore[import-untyped]

from jobcreator.utils import load_kubernetes_config, logger


def _setup_smb_pv(pv_name: str, secret_name: str, secret_namespace: str, source: str, mount_options: list[str]) -> None:
    """
    Sets up an smb PV using the loaded kubeconfig as a destination
    :param pv_name: str, The name given to the smb-pv when it's made
    :param secret_name: str, The name of the secret that contains the credentials for the smb share
    :param secret_namespace: str, the namespace of the secret
    :param source: str, The IP/url/uri that is used to mount the smb share
    :param mount_options: list, The mount options for the smb share
    :return: str, the name of the archive PV
    """
    metadata = client.V1ObjectMeta(name=pv_name, annotations={"pv.kubernetes.io/provisioned-by": "smb.csi.k8s.io"})
    secret_ref = client.V1SecretReference(name=secret_name, namespace=secret_namespace)
    csi = client.V1CSIPersistentVolumeSource(
        driver="smb.csi.k8s.io",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"source": source},
        node_stage_secret_ref=secret_ref,
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        persistent_volume_reclaim_policy="Retain",
        mount_options=mount_options,
        csi=csi,
    )
    archive_pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(archive_pv)


def _setup_pvc(pvc_name: str, pv_name: str, namespace: str, access_mode: str = "ReadOnlyMany") -> None:
    """
    Set up a PVC for the given pvc_name and pv_name in the given namespace
    :param pvc_name: str, The name of the pvc to make
    :param pv_name: str, The name of the pv to be claimed
    :param namespace: str, The namespace to create the pvc in
    """
    metadata = client.V1ObjectMeta(name=pvc_name)
    resources = client.V1ResourceRequirements(requests={"storage": "1000Gi"})
    spec = client.V1PersistentVolumeClaimSpec(
        access_modes=[access_mode],
        resources=resources,
        volume_name=pv_name,
        storage_class_name="",
    )
    archive_pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=metadata,
        spec=spec,
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(namespace=namespace, body=archive_pvc)


def _setup_extras_pvc(job_name: str, job_namespace: str, pv_name: str) -> str:
    """
    Sets up the extras Manila PVC using the loaded kubeconfig as a destination
    :param job_name: str, the name of the job that the PVC is made for
    :param job_namespace: str, the namespace that the job is in
    :param pv_name: str, the name of the PV the PVC is being made for
    :return: str, the name of the PVC
    """
    pvc_name = f"{job_name}-extras-pvc"
    metadata = client.V1ObjectMeta(name=pvc_name)
    resources = client.V1ResourceRequirements(requests={"storage": "1000Gi"})
    match_expression = client.V1LabelSelectorRequirement(key="name", operator="In", values=[pv_name])
    selector = client.V1LabelSelector(match_expressions=[match_expression])
    spec = client.V1PersistentVolumeClaimSpec(
        access_modes=["ReadOnlyMany"],
        resources=resources,
        selector=selector,
        storage_class_name="",
    )
    extras_pvc = client.V1PersistentVolumeClaim(
        api_version="v1",
        kind="PersistentVolumeClaim",
        metadata=metadata,
        spec=spec,
    )
    client.CoreV1Api().create_namespaced_persistent_volume_claim(namespace=job_namespace, body=extras_pvc)
    return pvc_name


def _setup_extras_pv(job_name: str, secret_namespace: str, manila_share_id: str, manila_share_access_id: str) -> str:
    """
    Setups up the extras PV using the loaded kubeconfig as destination
    :param job_name: str, the name of the job the PV is for
    :param manila_share_id: The id of the manila share to mount for extras
    :param manila_share_access_id: the id of the access rule for the manila share that provides access to the
    manila share
    :param secret_namespace: the namespace where the manila-creds secret is.
    :return: str, the name of the PV
    """
    pv_name = f"{job_name}-extras-pv"
    metadata = client.V1ObjectMeta(name=pv_name, labels={"name": pv_name})
    secret_ref = client.V1SecretReference(name="manila-creds", namespace=secret_namespace)
    csi = client.V1CSIPersistentVolumeSource(
        driver="cephfs.manila.csi.openstack.org",
        read_only=True,
        volume_handle=pv_name,
        volume_attributes={"shareID": manila_share_id, "shareAccessID": manila_share_access_id},
        node_stage_secret_ref=secret_ref,
        node_publish_secret_ref=secret_ref,
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadOnlyMany"],
        csi=csi,
    )
    archive_pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(archive_pv)
    return pv_name


def _setup_ceph_pv(
    pv_name: str,
    ceph_creds_k8s_secret_name: str,
    ceph_creds_k8s_namespace: str,
    cluster_id: str,
    fs_name: str,
    ceph_mount_path: str,
) -> str:
    """
    Sets up the ceph deneb PV using the loaded kubeconfig as a destination
    :param pv_name: str, the name of the PV
    :return: str, the name of the ceph deneb PV
    """
    metadata = client.V1ObjectMeta(name=pv_name)
    secret_ref = client.V1SecretReference(name=ceph_creds_k8s_secret_name, namespace=ceph_creds_k8s_namespace)
    csi = client.V1CSIPersistentVolumeSource(
        driver="cephfs.csi.ceph.com",
        node_stage_secret_ref=secret_ref,
        volume_handle=pv_name,
        volume_attributes={
            "clusterID": cluster_id,
            "mounter": "fuse",
            "fsName": fs_name,
            "staticVolume": "true",
            "rootPath": ceph_mount_path,
        },
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        storage_class_name="",
        access_modes=["ReadWriteMany"],
        persistent_volume_reclaim_policy="Retain",
        volume_mode="Filesystem",
        csi=csi,
    )
    ceph_pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(ceph_pv)
    return pv_name


def _setup_imat_pv_and_pvcs(job_name: str, namespace: str, pv_names: list[str], pvc_names: list[str]) -> None:
    imat_pv_name = f"{job_name}-ndximat-pv-smb"
    imat_pvc_name = f"{job_name}-ndximat-pvc"
    _setup_smb_pv(imat_pv_name, "imat-creds", namespace, "//NDXIMAT.isis.cclrc.ac.uk/data$/", [])
    _setup_pvc(imat_pvc_name, imat_pv_name, namespace)
    pv_names.append(imat_pv_name)
    pvc_names.append(imat_pvc_name)


def _generate_tolerations_from_taints(taints: list[dict[str, Any]]) -> list[client.V1Toleration]:
    tolerations = []
    for taint in taints:
        toleration = client.V1Toleration(
            value=taint.get("value", None),
            key=taint.get("key", None),
            operator=taint.get("operator", None),
            effect=taint.get("effect", None),
        )
        tolerations.append(toleration)
    return tolerations


def _generate_affinities(node_affinity_dict: dict[str, Any] | None = None) -> client.V1Affinity:
    # Add the anti-affinity that we always use
    pod_affinity_label_selector = client.V1LabelSelector(
        match_labels={"reduce.isis.cclrc.ac.uk/job-source": "automated-reduction"},
    )

    pod_affinity_term = client.V1PodAffinityTerm(
        topology_key="kubernetes.io/hostname",
        label_selector=pod_affinity_label_selector,
    )

    weighted_pod_affinity = client.V1WeightedPodAffinityTerm(weight=100, pod_affinity_term=pod_affinity_term)

    anti_affinity = client.V1PodAntiAffinity(
        preferred_during_scheduling_ignored_during_execution=[weighted_pod_affinity],
    )

    # Create new node affinities based on the list
    if node_affinity_dict is not None:
        node_affinity = client.V1NodeAffinity(
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
        return client.V1Affinity(pod_anti_affinity=anti_affinity, node_affinity=node_affinity)
    return client.V1Affinity(pod_anti_affinity=anti_affinity)


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self, watcher_sha: str, dev_mode: bool) -> None:
        """
        Takes the runner_sha and ensures that the kubernetes config is loaded before continuing.
        :param watcher_sha: str, The sha256 used for the watcher, often made by the watcher.D file in this repo's
        container folder
        :param dev_mode: bool, Whether the jobwatcher is launched in development mode
        :return: None
        """
        load_kubernetes_config()
        self.watcher_sha = watcher_sha
        self.dev_mode = dev_mode

    def spawn_job(  # noqa: PLR0913
        self,
        job_name: str,
        script: str,
        job_namespace: str,
        ceph_creds_k8s_secret_name: str,
        ceph_creds_k8s_namespace: str,
        cluster_id: str,
        fs_name: str,
        ceph_mount_path: str,
        job_id: int,
        max_time_to_complete_job: int,
        fia_api_host: str,
        fia_api_api_key: str,
        runner_image: str,
        manila_share_id: str,
        manila_share_access_id: str,
        special_pvs: list[str],
        taints: list[dict[str, Any]],
        affinity: dict[str, Any] | None,
    ) -> None:
        """
        Takes the meta_data from the message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param job_namespace: The namespace that the job should be created in
        :param ceph_creds_k8s_secret_name: The secret name of the ceph credentials
        :param ceph_creds_k8s_namespace: The secret namespace of the ceph credentials
        :param cluster_id: The cluster id for the ceph cluster to connect to
        :param fs_name: The file system name for the ceph cluster
        :param ceph_mount_path: the path on the ceph cluster to mount
        :param job_id: The id used in the DB for the reduction
        :param max_time_to_complete_job: The maximum time to allow for completion of a job in seconds
        :param fia_api_host: The fia api host for the fia cluster
        :param fia_api_api_key: The fia api key
        :param runner_image: the container image that has is to be used the containers have permission to use the
        directories required for outputting data.
        :param manila_share_id: The id of the manila share to mount for extras
        :param manila_share_access_id: the id of the access rule for the manila share that provides access to the
        manila share
        :param special_pvs: A list of special PV strings, that represent PVs that can be implemented.
        :param taints: A list of taints that the runner pods should have for example:
        [{"key": "gpu", "effect": "NoSchedule", "operator": "Exists"}]
        :param affinity: A dict that describes the node affinity of the job for example:
        {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}
        :return: None
        """
        logger.info("Creating PV and PVC for: %s", job_name)

        pv_names = []
        pvc_names = []

        # Setup Archive PV and PVC
        archive_pv_name = f"{job_name}-archive-pv-smb"
        _setup_smb_pv(
            archive_pv_name,
            "archive-creds",
            job_namespace,
            "//isisdatar55.isis.cclrc.ac.uk/inst$/",
            ["noserverino", "_netdev", "vers=2.1"],
        )
        pv_names.append(archive_pv_name)

        archive_pvc_name = f"{job_name}-archive-pvc"
        _setup_pvc(archive_pvc_name, archive_pv_name, job_namespace)
        pvc_names.append(archive_pvc_name)

        # Setup Extras PV and PVC
        extras_pv_name = _setup_extras_pv(
            job_name=job_name,
            secret_namespace=job_namespace,
            manila_share_id=manila_share_id,
            manila_share_access_id=manila_share_access_id,
        )
        pv_names.append(extras_pv_name)

        extras_pvc_name = f"{job_name}-extras-pvc"
        _setup_pvc(extras_pvc_name, extras_pv_name, job_namespace)
        pvc_names.append(extras_pvc_name)

        # Setup ceph PV and PVC
        if not self.dev_mode:
            ceph_pv_name = f"{job_name}-ceph-pv"
            (
                _setup_ceph_pv(
                    ceph_pv_name,
                    ceph_creds_k8s_secret_name,
                    ceph_creds_k8s_namespace,
                    cluster_id,
                    fs_name,
                    ceph_mount_path,
                ),
            )
            pv_names.append(ceph_pv_name)

            ceph_pvc_name = f"{job_name}-ceph-pvc"
            _setup_pvc(ceph_pvc_name, ceph_pv_name, job_namespace, access_mode="ReadWriteMany")
            pvc_names.append(ceph_pvc_name)

            ceph_volume = client.V1Volume(
                name="ceph-mount",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=ceph_pvc_name,
                    read_only=False,
                ),
            )
        else:
            ceph_volume = client.V1Volume(
                name="ceph-mount",
                empty_dir=client.V1EmptyDirVolumeSource(size_limit="100Gi"),
            )

        # Create the Job
        logger.info("Spawning job: %s", job_name)

        volumes = [
            client.V1Volume(
                name="archive-mount",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=archive_pvc_name,
                    read_only=True,
                ),
            ),
            ceph_volume,
            client.V1Volume(
                name="extras-mount",
                persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                    claim_name=extras_pvc_name,
                    read_only=True,
                ),
            ),
        ]
        volumes_mounts = [
            client.V1VolumeMount(name="archive-mount", mount_path="/archive"),
            client.V1VolumeMount(name="ceph-mount", mount_path="/output"),
            client.V1VolumeMount(name="extras-mount", mount_path="/extras"),
        ]
        # Setup special PVs and add them to the volume mounts
        if "imat" in special_pvs:
            _setup_imat_pv_and_pvcs(job_name, job_namespace, pv_names, pvc_names)
            imat_pvc_source = client.V1PersistentVolumeClaimVolumeSource(
                claim_name=f"{job_name}-ndximat-pvc", read_only=True
            )
            volumes.append(client.V1Volume(name="imat-mount", persistent_volume_claim=imat_pvc_source))
            volumes_mounts.append(client.V1VolumeMount(name="imat-mount", mount_path="/imat"))
            # Because imat is special and uses mantid imaging to load large .tiff files, we need to ensure the /dev/shm
            # is larger than 64mb. We do however have a soft-ish limit of around 32GiB on the size of datasets when
            # doing this.
            volumes.append(
                client.V1Volume(
                    name="dev-shm", empty_dir=client.V1EmptyDirVolumeSource(size_limit="32Gi", medium="Memory")
                )
            )
            volumes_mounts.append(client.V1VolumeMount(name="dev-shm", mount_path="/dev/shm"))  # noqa: S108

        main_container = client.V1Container(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=volumes_mounts,
        )

        watcher_container = client.V1Container(
            name="job-watcher",
            image=f"ghcr.io/fiaisis/jobwatcher@sha256:{self.watcher_sha}",
            env=[
                client.V1EnvVar(name="FIA_API_HOST", value=fia_api_host),
                client.V1EnvVar(name="FIA_API_API_KEY", value=fia_api_api_key),
                client.V1EnvVar(name="MAX_TIME_TO_COMPLETE_JOB", value=str(max_time_to_complete_job)),
                client.V1EnvVar(name="CONTAINER_NAME", value=job_name),
                client.V1EnvVar(name="JOB_NAME", value=job_name),
                client.V1EnvVar(name="POD_NAME", value=job_name),
            ],
        )

        affinity = _generate_affinities(node_affinity_dict=affinity)
        tolerations = _generate_tolerations_from_taints(taints)

        pod_spec = client.V1PodSpec(
            affinity=affinity,
            service_account_name="jobwatcher",
            containers=[main_container, watcher_container],
            restart_policy="Never",
            tolerations=tolerations,
            volumes=volumes,
        )

        pod_metadata = client.V1ObjectMeta(
            labels={"reduce.isis.cclrc.ac.uk/job-source": "automated-reduction"},
        )

        template = client.V1PodTemplateSpec(spec=pod_spec, metadata=pod_metadata)

        spec = client.V1JobSpec(
            template=template,
            backoff_limit=0,
            ttl_seconds_after_finished=21600,  # 6 hours
        )

        job_metadata = client.V1ObjectMeta(
            name=job_name,
            annotations={
                "job-id": str(job_id),
                "pvs": str(pv_names),
                "pvcs": str(pvc_names),
                "kubectl.kubernetes.io/default-container": main_container.name,
            },
        )

        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=job_metadata,
            spec=spec,
        )
        client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
