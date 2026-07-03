"""
The module is aimed to consume from a station on Memphis using the create_station_consumer
"""

import json
import time
from collections.abc import Callable
from typing import Any

from pika import BlockingConnection, ConnectionParameters, PlainCredentials  # type: ignore[import-untyped]

from jobcreator.utils import logger


class QueueConsumer:
    """
    This class is responsible for running the listener for RabbitMQ, and requesting the correct response from the
    JobController
    """

    def __init__(
        self,
        message_callback: Callable[[dict[str, Any]], None],
        queue_host: str,
        username: str,
        password: str,
        queue_name: str,
        failure_queue_name: str,
    ) -> None:
        self.message_callback = message_callback
        self.queue_host = queue_host
        self.queue_name = queue_name
        self.failure_queue_name = failure_queue_name
        credentials = PlainCredentials(username=username, password=password)
        self.connection_parameters = ConnectionParameters(queue_host, 5672, credentials=credentials)
        self.connection: Any = None
        self.channel: Any = None
        self.failure_connection: Any = None
        self.failure_channel: Any = None
        self.connect_to_broker()

    def _declare_queue(self, channel: Any, queue_name: str) -> None:
        channel.exchange_declare(
            queue_name,
            exchange_type="direct",
            durable=True,
        )
        channel.queue_declare(
            queue_name,
            durable=True,
            arguments={"x-queue-type": "quorum"},
        )
        channel.queue_bind(queue_name, queue_name, routing_key="")
        channel.basic_qos(prefetch_count=1)

    def connect_to_broker(self) -> None:
        """
        Use this to connect to the broker
        :return: None
        """
        self.connection = BlockingConnection(self.connection_parameters)
        self.channel = self.connection.channel()
        self._declare_queue(self.channel, self.queue_name)

        self.failure_connection = BlockingConnection(self.connection_parameters)
        self.failure_channel = self.failure_connection.channel()
        self._declare_queue(self.failure_channel, self.failure_queue_name)

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
            raise
        except Exception:
            logger.exception("Problem processing message callback")
            raise

    def _publish_failure(self, body: bytes) -> None:
        self.failure_channel.basic_publish(self.failure_queue_name, "", body)

    def _process_ingress_message(self) -> None:
        for header, _, body in self.channel.consume(
            self.queue_name,
            inactivity_timeout=5,
        ):
            try:
                self._message_handler(body.decode())
                self.channel.basic_ack(header.delivery_tag)
            except AttributeError:
                # If the message frame or body is missing attributes required e.g. the delivery tag
                pass
            except Exception:
                logger.warning("Problem processing message: %s", body)
                self._publish_failure(body)
                self.channel.basic_ack(header.delivery_tag)
            break

    def _process_failure_message(self) -> None:
        for header, _, body in self.failure_channel.consume(
            self.failure_queue_name,
            inactivity_timeout=5,
        ):
            try:
                self._message_handler(body.decode())
                self.failure_channel.basic_ack(header.delivery_tag)
            except AttributeError:
                # If the message frame or body is missing attributes required e.g. the delivery tag
                pass
            except Exception:
                logger.warning("Problem processing failed message: %s", body)
                self._publish_failure(body)
                self.failure_channel.basic_ack(header.delivery_tag)
            break

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
            callback_func()
            self._process_ingress_message()
            self._process_failure_message()

            time.sleep(0.1)
