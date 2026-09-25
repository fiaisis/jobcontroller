"""
Storage management and volume abstractions for Kubernetes jobs.
"""

from dataclasses import dataclass, field

from kubernetes import client  # type: ignore[import-untyped]


@dataclass
class VolumeBundle:
    """
    Encapsulates Kubernetes storage components needed for a job.

    Bundles pod-level volumes, container-level volume mounts, and any
    associated persistent volume (PV) and persistent volume claim (PVC)
    names that were created in the cluster.
    """

    volumes: list[client.V1Volume] = field(default_factory=list)
    volume_mounts: list[client.V1VolumeMount] = field(default_factory=list)
    pv_names: list[str] = field(default_factory=list)
    pvc_names: list[str] = field(default_factory=list)

    @classmethod
    def from_single(
        cls,
        volume: client.V1Volume,
        volume_mount: client.V1VolumeMount,
        pv_name: str | None = None,
        pvc_name: str | None = None,
    ) -> "VolumeBundle":
        """
        Convenience constructor for a single volume, mount, and optional PV/PVC names.

        :param volume: client.V1Volume, the Kubernetes Pod volume
        :param volume_mount: client.V1VolumeMount, the container volume mount
        :param pv_name: str | None, optional name of the created PV
        :param pvc_name: str | None, optional name of the created PVC
        :return: VolumeBundle, a bundle containing the single volume and mount
        """
        return cls(
            volumes=[volume],
            volume_mounts=[volume_mount],
            pv_names=[pv_name] if pv_name else [],
            pvc_names=[pvc_name] if pvc_name else [],
        )

    def extend(self, other: "VolumeBundle") -> None:
        """
        Merge another VolumeBundle into this bundle in place.

        :param other: VolumeBundle, the other bundle to merge into this one
        :return: None
        """
        self.volumes.extend(other.volumes)
        self.volume_mounts.extend(other.volume_mounts)
        self.pv_names.extend(other.pv_names)
        self.pvc_names.extend(other.pvc_names)

    def __add__(self, other: "VolumeBundle") -> "VolumeBundle":
        """
        Combine two VolumeBundles into a new VolumeBundle.

        :param other: VolumeBundle, the other bundle to concatenate
        :return: VolumeBundle, a new combined VolumeBundle
        """
        return VolumeBundle(
            volumes=self.volumes + other.volumes,
            volume_mounts=self.volume_mounts + other.volume_mounts,
            pv_names=self.pv_names + other.pv_names,
            pvc_names=self.pvc_names + other.pvc_names,
        )


def setup_smb_pv(
    pv_name: str, secret_name: str, secret_namespace: str, source: str, mount_options: list[str]
) -> None:
    """
    Sets up an SMB persistent volume using the loaded kubeconfig as destination.

    :param pv_name: str, The name given to the smb-pv when it is created
    :param secret_name: str, The name of the secret that contains the credentials for the SMB share
    :param secret_namespace: str, The namespace of the secret
    :param source: str, The IP/URL/URI used to mount the SMB share
    :param mount_options: list[str], The mount options for the SMB share
    :return: None
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


def setup_pvc(pvc_name: str, pv_name: str, namespace: str, access_mode: str = "ReadOnlyMany") -> None:
    """
    Sets up a persistent volume claim for the given pvc_name and pv_name in the given namespace.

    :param pvc_name: str, The name of the PVC to make
    :param pv_name: str, The name of the PV to be claimed
    :param namespace: str, The namespace to create the PVC in
    :param access_mode: str, The access mode for the PVC (default ReadOnlyMany)
    :return: None
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


def setup_manila_pv(
    pv_name: str,
    secret_namespace: str,
    manila_share_id: str,
    manila_share_access_id: str,
    read_only: bool = True,
    access_mode: str = "ReadOnlyMany",
) -> str:
    """
    Sets up a Manila PV using the loaded kubeconfig as destination.

    :param pv_name: str, The name of the PV to create
    :param secret_namespace: str, The namespace where the manila-creds secret is
    :param manila_share_id: str, The ID of the Manila share to mount
    :param manila_share_access_id: str, The ID of the access rule for the Manila share
    :param read_only: bool, Whether the volume should be mounted read-only (default True)
    :param access_mode: str, The access mode for the volume (default ReadOnlyMany)
    :return: str, The name of the created PV
    """
    metadata = client.V1ObjectMeta(name=pv_name, labels={"name": pv_name})
    secret_ref = client.V1SecretReference(name="manila-creds", namespace=secret_namespace)
    csi = client.V1CSIPersistentVolumeSource(
        driver="cephfs.manila.csi.openstack.org",
        read_only=read_only,
        volume_handle=pv_name,
        volume_attributes={"shareID": manila_share_id, "shareAccessID": manila_share_access_id},
        node_stage_secret_ref=secret_ref,
        node_publish_secret_ref=secret_ref,
    )
    spec = client.V1PersistentVolumeSpec(
        capacity={"storage": "1000Gi"},
        access_modes=[access_mode],
        csi=csi,
    )
    pv = client.V1PersistentVolume(api_version="v1", kind="PersistentVolume", metadata=metadata, spec=spec)
    client.CoreV1Api().create_persistent_volume(pv)
    return pv_name


def setup_extras_pv(job_name: str, secret_namespace: str, manila_share_id: str, manila_share_access_id: str) -> str:
    """
    Sets up the extras Manila PV using the loaded kubeconfig as destination.

    :param job_name: str, The name of the job the PV is for
    :param secret_namespace: str, The namespace where the manila-creds secret is
    :param manila_share_id: str, The ID of the Manila share to mount for extras
    :param manila_share_access_id: str, The ID of the access rule for the Manila share
    :return: str, The name of the created extras PV
    """
    pv_name = f"{job_name}-extras-pv"
    return setup_manila_pv(
        pv_name=pv_name,
        secret_namespace=secret_namespace,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        read_only=True,
        access_mode="ReadOnlyMany",
    )


def setup_extras_pvc(job_name: str, job_namespace: str, pv_name: str) -> str:
    """
    Sets up the extras Manila PVC using label selectors (legacy/compatibility).

    :param job_name: str, The name of the job that the PVC is made for
    :param job_namespace: str, The namespace that the job is in
    :param pv_name: str, The name of the PV the PVC is being made for
    :return: str, The name of the created extras PVC
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


def setup_ceph_pv(
    pv_name: str,
    ceph_creds_k8s_secret_name: str,
    ceph_creds_k8s_namespace: str,
    cluster_id: str,
    fs_name: str,
    ceph_mount_path: str,
) -> str:
    """
    Sets up the Ceph Deneb PV using the loaded kubeconfig as destination.

    :param pv_name: str, The name of the PV to create
    :param ceph_creds_k8s_secret_name: str, The secret name of the Ceph credentials
    :param ceph_creds_k8s_namespace: str, The secret namespace of the Ceph credentials
    :param cluster_id: str, The cluster ID for the Ceph cluster to connect to
    :param fs_name: str, The filesystem name for the Ceph cluster
    :param ceph_mount_path: str, The path on the Ceph cluster to mount
    :return: str, The name of the created Ceph Deneb PV
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


def setup_imat_pv_and_pvcs(job_name: str, namespace: str, pv_names: list[str], pvc_names: list[str]) -> None:
    """
    Sets up SMB PV and PVC for IMAT and tracks their names in the provided lists.

    :param job_name: str, The name of the job
    :param namespace: str, The namespace to create the resources in
    :param pv_names: list[str], List to append created PV names to
    :param pvc_names: list[str], List to append created PVC names to
    :return: None
    """
    imat_pv_name = f"{job_name}-ndximat-pv-smb"
    imat_pvc_name = f"{job_name}-ndximat-pvc"
    setup_smb_pv(imat_pv_name, "imat-creds", namespace, "//NDXIMAT.isis.cclrc.ac.uk/data$/", [])
    setup_pvc(imat_pvc_name, imat_pv_name, namespace)
    pv_names.append(imat_pv_name)
    pvc_names.append(imat_pvc_name)


def setup_gem_pv_and_pvcs(
    job_name: str,
    job_namespace: str,
    pv_names: list[str],
    pvc_names: list[str],
    manila_share_id: str | None = None,
    manila_share_access_id: str | None = None,
) -> None:
    """
    Sets up GEM PV and PVC and tracks their names in the provided lists.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace to create the resources in
    :param pv_names: list[str], List to append created PV names to
    :param pvc_names: list[str], List to append created PVC names to
    :param manila_share_id: str | None, Optional Manila share ID if backed by Manila PV
    :param manila_share_access_id: str | None, Optional Manila share access ID
    :return: None
    """
    gem_pv_name = f"{job_name}-ndxgem-pv"
    gem_pvc_name = f"{job_name}-ndxgem-pvc"
    if manila_share_id and manila_share_access_id:
        setup_manila_pv(
            pv_name=gem_pv_name,
            secret_namespace=job_namespace,
            manila_share_id=manila_share_id,
            manila_share_access_id=manila_share_access_id,
            read_only=False,
            access_mode="ReadWriteOnce",
        )
        setup_pvc(gem_pvc_name, gem_pv_name, job_namespace, access_mode="ReadWriteOnce")
    else:
        setup_pvc(gem_pvc_name, gem_pv_name, job_namespace)
    pv_names.append(gem_pv_name)
    pvc_names.append(gem_pvc_name)


def create_archive_bundle(job_name: str, job_namespace: str) -> VolumeBundle:
    """
    Sets up Archive SMB PV and PVC and returns the corresponding VolumeBundle.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace of the job
    :return: VolumeBundle, The archive volume bundle mounted at /archive
    """
    archive_pv_name = f"{job_name}-archive-pv-smb"
    setup_smb_pv(
        archive_pv_name,
        "archive-creds",
        job_namespace,
        "//isisdatar55.isis.cclrc.ac.uk/inst$/",
        ["noserverino", "_netdev", "vers=2.1"],
    )
    archive_pvc_name = f"{job_name}-archive-pvc"
    setup_pvc(archive_pvc_name, archive_pv_name, job_namespace)

    volume = client.V1Volume(
        name="archive-mount",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=archive_pvc_name,
            read_only=True,
        ),
    )
    volume_mount = client.V1VolumeMount(name="archive-mount", mount_path="/archive")
    return VolumeBundle.from_single(volume, volume_mount, archive_pv_name, archive_pvc_name)


def create_output_bundle(
    job_name: str,
    job_namespace: str,
    dev_mode: bool,
    ceph_creds_k8s_secret_name: str = "",
    ceph_creds_k8s_namespace: str = "",
    cluster_id: str = "",
    fs_name: str = "",
    ceph_mount_path: str = "",
) -> VolumeBundle:
    """
    Sets up the output volume bundle mounted at /output (Ceph in production, EmptyDir in dev_mode).

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace of the job
    :param dev_mode: bool, Whether development mode is active (uses emptyDir when True)
    :param ceph_creds_k8s_secret_name: str, Secret name for Ceph credentials
    :param ceph_creds_k8s_namespace: str, Secret namespace for Ceph credentials
    :param cluster_id: str, Ceph cluster ID
    :param fs_name: str, Ceph filesystem name
    :param ceph_mount_path: str, Ceph root mount path
    :return: VolumeBundle, The output volume bundle mounted at /output
    """
    if not dev_mode:
        ceph_pv_name = f"{job_name}-ceph-pv"
        setup_ceph_pv(
            ceph_pv_name,
            ceph_creds_k8s_secret_name,
            ceph_creds_k8s_namespace,
            cluster_id,
            fs_name,
            ceph_mount_path,
        )
        ceph_pvc_name = f"{job_name}-ceph-pvc"
        setup_pvc(ceph_pvc_name, ceph_pv_name, job_namespace, access_mode="ReadWriteMany")

        volume = client.V1Volume(
            name="ceph-mount",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=ceph_pvc_name,
                read_only=False,
            ),
        )
        volume_mount = client.V1VolumeMount(name="ceph-mount", mount_path="/output")
        return VolumeBundle.from_single(volume, volume_mount, ceph_pv_name, ceph_pvc_name)

    volume = client.V1Volume(
        name="ceph-mount",
        empty_dir=client.V1EmptyDirVolumeSource(size_limit="100Gi"),
    )
    volume_mount = client.V1VolumeMount(name="ceph-mount", mount_path="/output")
    return VolumeBundle.from_single(volume, volume_mount)


def create_extras_bundle(
    job_name: str,
    job_namespace: str,
    manila_share_id: str,
    manila_share_access_id: str,
) -> VolumeBundle:
    """
    Sets up Extras Manila PV and PVC and returns the corresponding VolumeBundle.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace of the job
    :param manila_share_id: str, The Manila share ID to mount
    :param manila_share_access_id: str, The Manila share access rule ID
    :return: VolumeBundle, The extras volume bundle mounted at /extras
    """
    extras_pv_name = setup_extras_pv(
        job_name=job_name,
        secret_namespace=job_namespace,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
    )
    extras_pvc_name = f"{job_name}-extras-pvc"
    setup_pvc(extras_pvc_name, extras_pv_name, job_namespace)

    volume = client.V1Volume(
        name="extras-mount",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
            claim_name=extras_pvc_name,
            read_only=True,
        ),
    )
    volume_mount = client.V1VolumeMount(name="extras-mount", mount_path="/extras")
    return VolumeBundle.from_single(volume, volume_mount, extras_pv_name, extras_pvc_name)


def create_imat_bundle(job_name: str, job_namespace: str) -> VolumeBundle:
    """
    Sets up IMAT storage (/imat SMB PVC and /dev/shm memory emptyDir) and returns the VolumeBundle.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace of the job
    :return: VolumeBundle, The IMAT volume bundle containing both /imat and /dev/shm
    """
    imat_pv_name = f"{job_name}-ndximat-pv-smb"
    imat_pvc_name = f"{job_name}-ndximat-pvc"
    setup_smb_pv(imat_pv_name, "imat-creds", job_namespace, "//NDXIMAT.isis.cclrc.ac.uk/data$/", [])
    setup_pvc(imat_pvc_name, imat_pv_name, job_namespace)

    imat_pvc_source = client.V1PersistentVolumeClaimVolumeSource(
        claim_name=imat_pvc_name, read_only=True
    )
    imat_volume = client.V1Volume(name="imat-mount", persistent_volume_claim=imat_pvc_source)
    imat_mount = client.V1VolumeMount(name="imat-mount", mount_path="/imat")

    shm_volume = client.V1Volume(
        name="dev-shm", empty_dir=client.V1EmptyDirVolumeSource(size_limit="32Gi", medium="Memory")
    )
    shm_mount = client.V1VolumeMount(name="dev-shm", mount_path="/dev/shm")

    return VolumeBundle(
        volumes=[imat_volume, shm_volume],
        volume_mounts=[imat_mount, shm_mount],
        pv_names=[imat_pv_name],
        pvc_names=[imat_pvc_name],
    )


def create_gem_bundle(
    job_name: str,
    job_namespace: str,
    manila_share_id: str | None = None,
    manila_share_access_id: str | None = None,
) -> VolumeBundle:
    """
    Sets up GEM storage with write access and returns the corresponding VolumeBundle.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace of the job
    :param manila_share_id: str | None, Optional Manila share ID
    :param manila_share_access_id: str | None, Optional Manila share access rule ID
    :return: VolumeBundle, The GEM volume bundle mounted at /gem with write permissions
    """
    gem_pv_name = f"{job_name}-ndxgem-pv"
    gem_pvc_name = f"{job_name}-ndxgem-pvc"
    if manila_share_id and manila_share_access_id:
        setup_manila_pv(
            pv_name=gem_pv_name,
            secret_namespace=job_namespace,
            manila_share_id=manila_share_id,
            manila_share_access_id=manila_share_access_id,
            read_only=False,
            access_mode="ReadWriteOnce",
        )
        setup_pvc(gem_pvc_name, gem_pv_name, job_namespace, access_mode="ReadWriteOnce")
    else:
        setup_pvc(gem_pvc_name, gem_pv_name, job_namespace)

    gem_pvc_source = client.V1PersistentVolumeClaimVolumeSource(
        claim_name=gem_pvc_name, read_only=False
    )
    volume = client.V1Volume(name="gem-mount", persistent_volume_claim=gem_pvc_source)
    volume_mount = client.V1VolumeMount(name="gem-mount", mount_path="/gem", read_only=False)
    return VolumeBundle.from_single(volume, volume_mount, gem_pv_name, gem_pvc_name)


def build_job_volumes(
    job_name: str,
    job_namespace: str,
    dev_mode: bool,
    manila_share_id: str,
    manila_share_access_id: str,
    special_pvs: list[str],
    ceph_creds_k8s_secret_name: str = "",
    ceph_creds_k8s_namespace: str = "",
    cluster_id: str = "",
    fs_name: str = "",
    ceph_mount_path: str = "",
) -> VolumeBundle:
    """
    Builds and combines all required storage volumes for a job into a single VolumeBundle.

    :param job_name: str, The name of the job
    :param job_namespace: str, The namespace that the job is created in
    :param dev_mode: bool, Whether development mode is active
    :param manila_share_id: str, The Manila share ID for extras (and GEM if applicable)
    :param manila_share_access_id: str, The Manila share access ID
    :param special_pvs: list[str], List of special volume identifiers (e.g. 'imat', 'gem')
    :param ceph_creds_k8s_secret_name: str, Secret name for Ceph credentials
    :param ceph_creds_k8s_namespace: str, Secret namespace for Ceph credentials
    :param cluster_id: str, Cluster ID for the Ceph cluster
    :param fs_name: str, Filesystem name for Ceph
    :param ceph_mount_path: str, Mount path on Ceph
    :return: VolumeBundle, The consolidated volume bundle containing all volumes, mounts, and PV/PVC names
    """
    bundle = VolumeBundle()

    # 1. Archive bundle (SMB)
    bundle.extend(create_archive_bundle(job_name, job_namespace))

    # 2. Output bundle (Ceph or emptyDir)
    bundle.extend(
        create_output_bundle(
            job_name=job_name,
            job_namespace=job_namespace,
            dev_mode=dev_mode,
            ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
            ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
            cluster_id=cluster_id,
            fs_name=fs_name,
            ceph_mount_path=ceph_mount_path,
        )
    )

    # 3. Extras bundle (Manila)
    bundle.extend(
        create_extras_bundle(
            job_name=job_name,
            job_namespace=job_namespace,
            manila_share_id=manila_share_id,
            manila_share_access_id=manila_share_access_id,
        )
    )

    # 4. Special PVs
    if "imat" in special_pvs:
        bundle.extend(create_imat_bundle(job_name, job_namespace))

    if "gem" in special_pvs:
        bundle.extend(
            create_gem_bundle(
                job_name=job_name,
                job_namespace=job_namespace,
                manila_share_id=manila_share_id,
                manila_share_access_id=manila_share_access_id,
            )
        )

    return bundle


# Backwards compatibility aliases
_setup_smb_pv = setup_smb_pv
_setup_pvc = setup_pvc
_setup_extras_pv = setup_extras_pv
_setup_extras_pvc = setup_extras_pvc
_setup_ceph_pv = setup_ceph_pv
_setup_imat_pv_and_pvcs = setup_imat_pv_and_pvcs
_setup_gem_pv_and_pvcs = setup_gem_pv_and_pvcs
