"""
The RunMaker is responsible for creating k8s pods that perform the reduction. It expects the kafka IP to be present in
the environment as KAFKA_IP.
"""
import logging
import os
import sys

from jobcontroller.k8sapi import K8sAPI
from jobcontroller.topicconsumer import TopicConsumer

file_handler = logging.FileHandler(filename="run-detection.log")
stdout_handler = logging.StreamHandler(stream=sys.stdout)
logging.basicConfig(
    handlers=[file_handler, stdout_handler],
    format="[%(asctime)s]-%(name)s-%(levelname)s: %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


class JobController:
    """
    This is the JobController class that will communicate between the consumer and the kubernetes API, it effectively
    functions as a main class.
    """

    def __init__(self) -> None:
        self.kafka_ip = os.environ.get("KAFKA_IP", "broker")
        self.consumer = TopicConsumer(self.on_message, broker_ip=self.kafka_ip)
        self.k8s = K8sAPI()

    def on_message(self, message: dict) -> None:
        """
        Request that the k8s api spawns a pod
        :param message: dict, the message is a dictionary containing the needed information for spawning a pod
        :return: None
        """
        filename = os.path.basename(message["filepath"])
        rb_number = message["exeriment_number"]
        instrument_name = message["instrument"]
        self.k8s.spawn_pod(
            filename=filename, kafka_ip=self.kafka_ip, rb_number=rb_number, instrument_name=instrument_name
        )

    def run(self) -> None:
        """
        This is effectively the main method of the program and starts the consumer
        """
        self.consumer.start_consuming()


if __name__ == "__main__":
    job_controller = JobController()
    job_controller.run()
