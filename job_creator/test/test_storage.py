from unittest import mock
from unittest.mock import call

from jobcreator.storage import (
    VolumeBundle,
    build_job_volumes,
    create_archive_bundle,
    create_extras_bundle,
    create_gem_bundle,
    create_imat_bundle,
    create_output_bundle,
    setup_ceph_pv,
    setup_extras_pv,
    setup_extras_pvc,
    setup_gem_pv_and_pvcs,
    setup_imat_pv_and_pvcs,
    setup_manila_pv,
    setup_pvc,
    setup_smb_pv,
)


def test_volume_bundle_from_single():
    volume = mock.MagicMock()
    mount = mock.MagicMock()
    bundle = VolumeBundle.from_single(volume, mount, pv_name="my-pv", pvc_name="my-pvc")

    assert bundle.volumes == [volume]
    assert bundle.volume_mounts == [mount]
    assert bundle.pv_names == ["my-pv"]
    assert bundle.pvc_names == ["my-pvc"]


def test_volume_bundle_extend():
    bundle1 = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv1", "pvc1")
    bundle2 = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv2", "pvc2")

    bundle1.extend(bundle2)
    assert len(bundle1.volumes) == 2
    assert len(bundle1.volume_mounts) == 2
    assert bundle1.pv_names == ["pv1", "pv2"]
    assert bundle1.pvc_names == ["pvc1", "pvc2"]


def test_volume_bundle_add():
    bundle1 = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv1", "pvc1")
    bundle2 = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv2", "pvc2")

    combined = bundle1 + bundle2
    assert len(combined.volumes) == 2
    assert len(combined.volume_mounts) == 2
    assert combined.pv_names == ["pv1", "pv2"]
    assert combined.pvc_names == ["pvc1", "pvc2"]


@mock.patch("jobcreator.storage.client")
def test_setup_smb_pv(client):
    secret_namespace = mock.MagicMock()
    mount_options = mock.MagicMock()
    source = mock.MagicMock()
    pv_name = mock.MagicMock()
    secret_name = mock.MagicMock()

    setup_smb_pv(
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


@mock.patch("jobcreator.storage.client")
def test_setup_pvc_default_access_mode(client):
    pvc_name = mock.MagicMock()
    pv_name = mock.MagicMock()
    namespace = mock.MagicMock()

    setup_pvc(pvc_name, pv_name, namespace)

    client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
    client.V1ResourceRequirements.assert_called_once_with(requests={"storage": "1000Gi"})
    client.V1PersistentVolumeClaimSpec.assert_called_once_with(
        access_modes=["ReadOnlyMany"],
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
    client.CoreV1Api.return_value.create_namespaced_persistent_volume_claim.assert_called_once_with(
        namespace=namespace, body=client.V1PersistentVolumeClaim.return_value
    )


@mock.patch("jobcreator.storage.client")
def test_setup_pvc_custom_access_mode(client):
    pvc_name = mock.MagicMock()
    pv_name = mock.MagicMock()
    namespace = mock.MagicMock()

    setup_pvc(pvc_name, pv_name, namespace, access_mode="ReadWriteOnce")

    client.V1PersistentVolumeClaimSpec.assert_called_once_with(
        access_modes=["ReadWriteOnce"],
        resources=client.V1ResourceRequirements.return_value,
        volume_name=pv_name,
        storage_class_name="",
    )


@mock.patch("jobcreator.storage.client")
def test_setup_manila_pv_read_only(client):
    pv_name = mock.MagicMock()
    secret_namespace = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()

    result = setup_manila_pv(
        pv_name=pv_name,
        secret_namespace=secret_namespace,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        read_only=True,
        access_mode="ReadOnlyMany",
    )

    assert result == pv_name
    client.CoreV1Api.return_value.create_persistent_volume.assert_called_once_with(
        client.V1PersistentVolume.return_value,
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


@mock.patch("jobcreator.storage.client")
def test_setup_manila_pv_read_write(client):
    pv_name = mock.MagicMock()
    secret_namespace = mock.MagicMock()
    manila_share_id = mock.MagicMock()
    manila_share_access_id = mock.MagicMock()

    result = setup_manila_pv(
        pv_name=pv_name,
        secret_namespace=secret_namespace,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        read_only=False,
        access_mode="ReadWriteOnce",
    )

    assert result == pv_name
    client.V1PersistentVolumeSpec.assert_called_once_with(
        capacity={"storage": "1000Gi"},
        access_modes=["ReadWriteOnce"],
        csi=client.V1CSIPersistentVolumeSource.return_value,
    )
    client.V1CSIPersistentVolumeSource.assert_called_once_with(
        driver="cephfs.manila.csi.openstack.org",
        read_only=False,
        volume_handle=pv_name,
        volume_attributes={"shareID": manila_share_id, "shareAccessID": manila_share_access_id},
        node_stage_secret_ref=client.V1SecretReference.return_value,
        node_publish_secret_ref=client.V1SecretReference.return_value,
    )


@mock.patch("jobcreator.storage.setup_manila_pv")
def test_setup_extras_pv(mock_setup_manila):
    job_name = "test-job"
    secret_namespace = "test-ns"
    manila_share_id = "share-123"
    manila_share_access_id = "access-123"

    mock_setup_manila.return_value = "test-job-extras-pv"
    result = setup_extras_pv(job_name, secret_namespace, manila_share_id, manila_share_access_id)

    assert result == "test-job-extras-pv"
    mock_setup_manila.assert_called_once_with(
        pv_name="test-job-extras-pv",
        secret_namespace=secret_namespace,
        manila_share_id=manila_share_id,
        manila_share_access_id=manila_share_access_id,
        read_only=True,
        access_mode="ReadOnlyMany",
    )


@mock.patch("jobcreator.storage.client")
def test_setup_extras_pvc(client):
    job_name = str(mock.MagicMock())
    job_namespace = str(mock.MagicMock())
    pvc_name = f"{job_name}-extras-pvc"
    pv_name = mock.MagicMock()

    assert setup_extras_pvc(job_name, job_namespace, pv_name) == pvc_name

    client.V1ObjectMeta.assert_called_once_with(name=pvc_name)
    client.V1ResourceRequirements.assert_called_once_with(requests={"storage": "1000Gi"})
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


@mock.patch("jobcreator.storage.client")
def test_setup_ceph_pv(client):
    pv_name = mock.MagicMock()
    ceph_creds_k8s_secret_name = mock.MagicMock()
    ceph_creds_k8s_namespace = mock.MagicMock()
    cluster_id = mock.MagicMock()
    fs_name = mock.MagicMock()
    ceph_mount_path = mock.MagicMock()

    assert (
        setup_ceph_pv(
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


@mock.patch("jobcreator.storage.setup_pvc")
@mock.patch("jobcreator.storage.setup_smb_pv")
def test_setup_imat_pv_and_pvcs(mock_smb, mock_pvc):
    pv_names = []
    pvc_names = []
    setup_imat_pv_and_pvcs("job-1", "ns-1", pv_names, pvc_names)

    assert pv_names == ["job-1-ndximat-pv-smb"]
    assert pvc_names == ["job-1-ndximat-pvc"]
    mock_smb.assert_called_once_with(
        "job-1-ndximat-pv-smb", "imat-creds", "ns-1", "//NDXIMAT.isis.cclrc.ac.uk/data$/", []
    )
    mock_pvc.assert_called_once_with("job-1-ndximat-pvc", "job-1-ndximat-pv-smb", "ns-1")


@mock.patch("jobcreator.storage.setup_pvc")
def test_setup_gem_pv_and_pvcs_default(mock_pvc):
    pv_names = []
    pvc_names = []
    setup_gem_pv_and_pvcs("job-1", "ns-1", pv_names, pvc_names)

    assert pv_names == ["job-1-ndxgem-pv"]
    assert pvc_names == ["job-1-ndxgem-pvc"]
    mock_pvc.assert_called_once_with("job-1-ndxgem-pvc", "job-1-ndxgem-pv", "ns-1")


@mock.patch("jobcreator.storage.setup_manila_pv")
@mock.patch("jobcreator.storage.setup_pvc")
def test_setup_gem_pv_and_pvcs_with_manila(mock_pvc, mock_manila):
    pv_names = []
    pvc_names = []
    setup_gem_pv_and_pvcs("job-1", "ns-1", pv_names, pvc_names, manila_share_id="s1", manila_share_access_id="a1")

    assert pv_names == ["job-1-ndxgem-pv"]
    assert pvc_names == ["job-1-ndxgem-pvc"]
    mock_manila.assert_called_once_with(
        pv_name="job-1-ndxgem-pv",
        secret_namespace="ns-1",
        manila_share_id="s1",
        manila_share_access_id="a1",
        read_only=False,
        access_mode="ReadWriteOnce",
    )
    mock_pvc.assert_called_once_with("job-1-ndxgem-pvc", "job-1-ndxgem-pv", "ns-1", access_mode="ReadWriteOnce")


@mock.patch("jobcreator.storage.client")
@mock.patch("jobcreator.storage.setup_pvc")
@mock.patch("jobcreator.storage.setup_smb_pv")
def test_create_archive_bundle(mock_smb, mock_pvc, client):
    bundle = create_archive_bundle("job-1", "ns-1")

    mock_smb.assert_called_once_with(
        "job-1-archive-pv-smb",
        "archive-creds",
        "ns-1",
        "//isisdatar55.isis.cclrc.ac.uk/inst$/",
        ["noserverino", "_netdev", "vers=2.1"],
    )
    mock_pvc.assert_called_once_with("job-1-archive-pvc", "job-1-archive-pv-smb", "ns-1")
    assert bundle.pv_names == ["job-1-archive-pv-smb"]
    assert bundle.pvc_names == ["job-1-archive-pvc"]
    client.V1Volume.assert_called_once_with(
        name="archive-mount",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value,
    )
    client.V1PersistentVolumeClaimVolumeSource.assert_called_once_with(
        claim_name="job-1-archive-pvc",
        read_only=True,
    )
    client.V1VolumeMount.assert_called_once_with(name="archive-mount", mount_path="/archive")


@mock.patch("jobcreator.storage.client")
@mock.patch("jobcreator.storage.setup_pvc")
@mock.patch("jobcreator.storage.setup_ceph_pv")
def test_create_output_bundle_production(mock_ceph, mock_pvc, client):
    bundle = create_output_bundle(
        job_name="job-1",
        job_namespace="ns-1",
        dev_mode=False,
        ceph_creds_k8s_secret_name="secret",
        ceph_creds_k8s_namespace="ns-1",
        cluster_id="cid",
        fs_name="fs",
        ceph_mount_path="/mnt",
    )

    mock_ceph.assert_called_once_with("job-1-ceph-pv", "secret", "ns-1", "cid", "fs", "/mnt")
    mock_pvc.assert_called_once_with("job-1-ceph-pvc", "job-1-ceph-pv", "ns-1", access_mode="ReadWriteMany")
    assert bundle.pv_names == ["job-1-ceph-pv"]
    assert bundle.pvc_names == ["job-1-ceph-pvc"]
    client.V1Volume.assert_called_once_with(
        name="ceph-mount",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value,
    )
    client.V1VolumeMount.assert_called_once_with(name="ceph-mount", mount_path="/output")


@mock.patch("jobcreator.storage.client")
def test_create_output_bundle_dev_mode(client):
    bundle = create_output_bundle(job_name="job-1", job_namespace="ns-1", dev_mode=True)

    assert bundle.pv_names == []
    assert bundle.pvc_names == []
    client.V1Volume.assert_called_once_with(
        name="ceph-mount",
        empty_dir=client.V1EmptyDirVolumeSource.return_value,
    )
    client.V1EmptyDirVolumeSource.assert_called_once_with(size_limit="100Gi")
    client.V1VolumeMount.assert_called_once_with(name="ceph-mount", mount_path="/output")


@mock.patch("jobcreator.storage.client")
@mock.patch("jobcreator.storage.setup_pvc")
@mock.patch("jobcreator.storage.setup_extras_pv")
def test_create_extras_bundle(mock_extras_pv, mock_pvc, client):
    mock_extras_pv.return_value = "job-1-extras-pv"
    bundle = create_extras_bundle("job-1", "ns-1", "share-1", "access-1")

    mock_extras_pv.assert_called_once_with(
        job_name="job-1", secret_namespace="ns-1", manila_share_id="share-1", manila_share_access_id="access-1"
    )
    mock_pvc.assert_called_once_with("job-1-extras-pvc", "job-1-extras-pv", "ns-1")
    assert bundle.pv_names == ["job-1-extras-pv"]
    assert bundle.pvc_names == ["job-1-extras-pvc"]
    client.V1Volume.assert_called_once_with(
        name="extras-mount",
        persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource.return_value,
    )
    client.V1VolumeMount.assert_called_once_with(name="extras-mount", mount_path="/extras")


@mock.patch("jobcreator.storage.client")
@mock.patch("jobcreator.storage.setup_pvc")
@mock.patch("jobcreator.storage.setup_smb_pv")
def test_create_imat_bundle(mock_smb, mock_pvc, client):
    bundle = create_imat_bundle("job-1", "ns-1")

    mock_smb.assert_called_once_with(
        "job-1-ndximat-pv-smb", "imat-creds", "ns-1", "//NDXIMAT.isis.cclrc.ac.uk/data$/", []
    )
    mock_pvc.assert_called_once_with("job-1-ndximat-pvc", "job-1-ndximat-pv-smb", "ns-1")
    assert bundle.pv_names == ["job-1-ndximat-pv-smb"]
    assert bundle.pvc_names == ["job-1-ndximat-pvc"]
    assert len(bundle.volumes) == 2
    assert len(bundle.volume_mounts) == 2


@mock.patch("jobcreator.storage.client")
@mock.patch("jobcreator.storage.setup_pvc")
def test_create_gem_bundle(mock_pvc, client):
    bundle = create_gem_bundle("job-1", "ns-1")

    mock_pvc.assert_called_once_with("job-1-ndxgem-pvc", "job-1-ndxgem-pv", "ns-1")
    assert bundle.pv_names == ["job-1-ndxgem-pv"]
    assert bundle.pvc_names == ["job-1-ndxgem-pvc"]
    client.V1VolumeMount.assert_called_once_with(name="gem-mount", mount_path="/gem", read_only=False)


@mock.patch("jobcreator.storage.create_gem_bundle")
@mock.patch("jobcreator.storage.create_imat_bundle")
@mock.patch("jobcreator.storage.create_extras_bundle")
@mock.patch("jobcreator.storage.create_output_bundle")
@mock.patch("jobcreator.storage.create_archive_bundle")
def test_build_job_volumes(mock_archive, mock_output, mock_extras, mock_imat, mock_gem):
    mock_archive.return_value = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv-arch", "pvc-arch")
    mock_output.return_value = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv-out", "pvc-out")
    mock_extras.return_value = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv-ext", "pvc-ext")
    mock_imat.return_value = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv-imat", "pvc-imat")
    mock_gem.return_value = VolumeBundle.from_single(mock.MagicMock(), mock.MagicMock(), "pv-gem", "pvc-gem")

    bundle = build_job_volumes(
        job_name="job-1",
        job_namespace="ns-1",
        dev_mode=False,
        manila_share_id="share-1",
        manila_share_access_id="acc-1",
        special_pvs=["imat", "gem"],
    )

    mock_archive.assert_called_once_with("job-1", "ns-1")
    mock_output.assert_called_once()
    mock_extras.assert_called_once_with(
        job_name="job-1", job_namespace="ns-1", manila_share_id="share-1", manila_share_access_id="acc-1"
    )
    mock_imat.assert_called_once_with("job-1", "ns-1")
    mock_gem.assert_called_once_with(
        job_name="job-1", job_namespace="ns-1", manila_share_id="share-1", manila_share_access_id="acc-1"
    )
    assert bundle.pv_names == ["pv-arch", "pv-out", "pv-ext", "pv-imat", "pv-gem"]
    assert bundle.pvc_names == ["pvc-arch", "pvc-out", "pvc-ext", "pvc-imat", "pvc-gem"]
