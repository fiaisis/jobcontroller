"""
The module is aimed to consume from a station on Memphis using the create_station_consumer
"""

import json
import time
from collections.abc import Callable

import pika  # type: ignore[import]
from pika import BlockingConnection, ConnectionParameters, PlainCredentials

from jobcreator.utils import logger


class QueueConsumer:
    """
    This class is responsible for running the listener for RabbitMQ, and requesting the correct response from the
    JobController
    """

    def __init__(
        self,
        message_callback: Callable[[dict[str, str]], None],
        queue_host: str,
        username: str,
        password: str,
        queue_name: str,
    ) -> None:
        self.message_callback = message_callback
        self.queue_host = queue_host
        self.queue_name = queue_name
        credentials = PlainCredentials(username=username, password=password)
        self.connection_parameters = ConnectionParameters(queue_host, 5672, credentials=credentials)
        self.connection = None
        self.channel = None
        self.connect_to_broker()

    def connect_to_broker(self, max_retries: int = 10) -> None:
        """
        Use this to connect to the broker
        :return: None
        """
        for attempt in range(max_retries):
            try:
                self.connection = BlockingConnection(self.connection_parameters)
                self.channel = self.connection.channel()  # type: ignore[attr-defined]
                self.channel.exchange_declare(  # type: ignore[attr-defined]
                    self.queue_name,
                    exchange_type="direct",
                    durable=True,
                )
                self.channel.queue_declare(  # type: ignore[attr-defined]
                    self.queue_name,
                    durable=True,
                    arguments={"x-queue-type": "quorum"},
                )
                self.channel.queue_bind(self.queue_name, self.queue_name, routing_key="")  # type: ignore[attr-defined]
            except pika.exceptions.AMQPConnectionError:
                wait_time = min(2**attempt, 30)
                logger.warning(
                    "Broker unavailable (attempt %d/%d), retrying in %ds", attempt + 1, max_retries, wait_time
                )
                time.sleep(wait_time)
        raise RuntimeError(f"Failed to connect to message broker after {max_retries} attempts")

    def _message_handler(self, msg: str) -> None:
        """
        Handles a message from the message broker
        :param msg: A message that need to be processed
        :return: None
        """
        try:
            msg_obj = json.loads(msg)
            logger.info("Message decoded as: %s", msg_obj)
            self.message_callback(msg_obj)
        except json.JSONDecodeError as exception:
            logger.error("Error attempting to decode JSON: %s", str(exception))

    def start_consuming(self, callback_func: Callable[[], None], run_once: bool = False) -> None:
        """
        The function that will start consuming from a queue, and when the consumer receives a message.
        :param callback_func: This function is called once per loop
        :param run_once: Should this only run once or run until there is a raised exception or interrupt.
        :return: None
        """
        run = True
        while run:
            if run_once:
                run = False
            try:
                callback_func()
                for header, _, body in self.channel.consume(  # type: ignore[attr-defined]
                    self.queue_name,
                    inactivity_timeout=5,
                ):
                    try:
                        self._message_handler(body.decode())
                        self.channel.basic_ack(header.delivery_tag)  # type: ignore[attr-defined]
                    except AttributeError:
                        # If the message frame or body is missing attributes required e.g. the delivery tag
                        pass
                    except Exception:
                        logger.warning("Problem processing message: %s", body)
                    break
                time.sleep(0.1)
            except (pika.exceptions.AMQPConnectionError, pika.exceptions.ChannelCloserByBroker) as e:
                logger.warning("Lost connection to broker: %s. Reconnecting...", e)
                time.sleep(5)
                self.connect_to_broker()
