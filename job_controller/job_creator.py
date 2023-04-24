"""
Communicate to a kubernetes API to spawn a pod with the metadata passed by kafka message to the RunMaker
"""
from kubernetes import client  # type: ignore[import]

from job_controller.utils import logger, load_kubernetes_config


class JobCreator:
    """
    This class is responsible for loading the kubernetes config and handling methods for creating new pods.
    """

    def __init__(self) -> None:
        load_kubernetes_config()

    @staticmethod
    def spawn_job(job_name: str, script: str, ceph_path: str, job_namespace: str, user_id: str) -> str:
        """
        Takes the meta_data from the kafka message and uses that dictionary for generating the deployment of the pod.
        :param job_name: The name that the job should be created as
        :param script: The script that should be executed
        :param ceph_path: The path to which that should be mounted at /output in the job, this is expected to be the
        RBNumber folder that data will be dumped into.
        :param job_namespace: The namespace that the job should be created inside of
        :param user_id: The autoreduce user's user id, this is used primarily for mounting CEPH and will ensure that
        the containers have permission to use the directories required for outputting data.
        :return: The response for the actual job's name
        """
        logger.info("Spawning job: %s", job_name)
        job = client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata={"name": job_name},
            spec={
                "ttlSecondsAfterFinished": 21600,  # 6 hours
                "template": {
                    "spec": {
                        "security_context": {
                            "runAsUser": user_id,
                        },
                        "containers": [
                            {
                                "name": job_name,
                                "image": "ghcr.io/interactivereduction/runner@"
                                "sha256:c18868a1aecfee5cc4b91ff55944b1104636d99865e0b9dc789c9d16e54a73a3",
                                "command": ["python"],
                                "args": ["-c", script],
                                "volumeMounts": [
                                    {"name": "archive-mount", "mountPath": "/archive"},
                                    {"name": "ceph-mount", "mountPath": "/output"},
                                ],
                            }
                        ],
                        "restartPolicy": "Never",
                        "tolerations": [{"key": "queue-worker", "effect": "NoSchedule", "operator": "Exists"}],
                        "volumes": [
                            {"name": "archive-mount", "hostPath": {"type": "Directory", "path": "/archive"}},
                            {"name": "ceph-mount", "hostPath": {"type": "Directory", "path": ceph_path}},
                        ],
                    },
                },
            },
        )
        response = client.BatchV1Api().create_namespaced_job(namespace=job_namespace, body=job)
        return str(response.metadata.name)
