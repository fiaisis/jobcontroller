"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by message to the RunMaker
"""

from typing import Any

from kubernetes import client  # type: ignore[import-untyped]

from jobcreator.storage import (
    VolumeBundle,
    _setup_ceph_pv,
    _setup_extras_pv,
    _setup_extras_pvc,
    _setup_gem_pv_and_pvcs,
    _setup_imat_pv_and_pvcs,
    _setup_pvc,
    _setup_smb_pv,
    build_job_volumes,
)
from jobcreator.utils import load_kubernetes_config, logger


def _generate_tolerations_from_taints(taints: list[dict[str, Any]] | None = None) -> list[client.V1Toleration]:
    tolerations = []
    if not taints:
        return tolerations
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
    if node_affinity_dict is not None and node_affinity_dict != {}:
        expected_keys = ["key", "operator", "values"]
        for expected_key in expected_keys:
            if expected_key not in node_affinity_dict:
                logger.error(
                    "Expected key for node affinity not found: %s. Spawning without node affinity.", expected_key
                )
                return client.V1Affinity(pod_anti_affinity=anti_affinity)
        node_affinity = client.V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=client.V1NodeSelector(
                node_selector_terms=[
                    client.V1NodeSelectorTerm(
                        match_expressions=[
                            client.V1NodeSelectorRequirement(
                                key=node_affinity_dict["key"],
                                operator=node_affinity_dict["operator"],
                                values=node_affinity_dict["values"],
                            )
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
        ceph_creds_k8s_secret_name: str = "",
        ceph_creds_k8s_namespace: str = "",
        cluster_id: str = "",
        fs_name: str = "",
        ceph_mount_path: str = "",
        job_id: int = 0,
        max_time_to_complete_job: int = 0,
        fia_api_host: str = "",
        fia_api_api_key: str = "",
        runner_image: str = "",
        manila_share_id: str = "",
        manila_share_access_id: str = "",
        special_pvs: list[str] | None = None,
        taints: list[dict[str, Any]] | None = None,
        affinity: dict[str, Any] | None = None,
        storage_bundle: VolumeBundle | None = None,
        gpu_job: bool | None = None,
    ) -> None:
        """
        Takes the meta_data from the message and uses that dictionary for generating the deployment of the pod.
        :param job_name: str, The name that the job should be created as
        :param script: str, The script that should be executed
        :param job_namespace: str, The namespace that the job should be created in
        :param ceph_creds_k8s_secret_name: str, The secret name of the ceph credentials
        :param ceph_creds_k8s_namespace: str, The secret namespace of the ceph credentials
        :param cluster_id: str, The cluster id for the ceph cluster to connect to
        :param fs_name: str, The file system name for the ceph cluster
        :param ceph_mount_path: str, the path on the ceph cluster to mount
        :param job_id: int, The id used in the DB for the reduction
        :param max_time_to_complete_job: int, The maximum time to allow for completion of a job in seconds
        :param fia_api_host: str, The fia api host for the fia cluster
        :param fia_api_api_key: str, The fia api key
        :param runner_image: str, the container image that has is to be used the containers have permission to use the
        directories required for outputting data.
        :param manila_share_id: str, The id of the manila share to mount for extras
        :param manila_share_access_id: str, the id of the access rule for the manila share that provides access to the
        manila share
        :param special_pvs: list[str] | None, A list of special PV strings, that represent PVs that can be implemented.
        :param taints: list[dict[str, Any]] | None, A list of taints that the runner pods should have for example:
        [{"key": "gpu", "effect": "NoSchedule", "operator": "Exists"}]
        :param affinity: dict[str, Any] | None, A dict that describes the node affinity of the job for example:
        {"key": "node-type", "operator": "In", "values": ["gpu-worker"]}
        :param storage_bundle: VolumeBundle | None, Pre-assembled volume bundle; if omitted, built from
        storage parameters
        :param gpu_job: bool | None, Whether this is a GPU workload (defaults to checking special_pvs for 'imat')
        :return: None
        """
        if storage_bundle is None:
            logger.info("Creating PV and PVC for: %s", job_name)
            storage_bundle = build_job_volumes(
                job_name=job_name,
                job_namespace=job_namespace,
                dev_mode=self.dev_mode,
                manila_share_id=manila_share_id,
                manila_share_access_id=manila_share_access_id,
                special_pvs=special_pvs or [],
                ceph_creds_k8s_secret_name=ceph_creds_k8s_secret_name,
                ceph_creds_k8s_namespace=ceph_creds_k8s_namespace,
                cluster_id=cluster_id,
                fs_name=fs_name,
                ceph_mount_path=ceph_mount_path,
            )

        if gpu_job is None:
            gpu_job = special_pvs is not None and "imat" in special_pvs

        logger.info("Spawning job: %s", job_name)

        main_container = client.V1Container(
            name=job_name,
            image=runner_image,
            args=[script],
            env=[client.V1EnvVar(name="PYTHONUNBUFFERED", value="1")],
            volume_mounts=storage_bundle.volume_mounts,
            resources=client.V1ResourceRequirements(
                limits={"nvidia.com/gpu": "1"},
            )
            if gpu_job
            else None,
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
            volumes=storage_bundle.volumes,
            runtime_class_name="nvidia" if gpu_job else None,
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
                "pvs": str(storage_bundle.pv_names),
                "pvcs": str(storage_bundle.pvc_names),
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
