import json
from types import SimpleNamespace
from unittest import mock

import pytest
from pika import PlainCredentials

from jobcreator.queue_consumer import QueueConsumer

QUEUE_HOST = "queue-host"
USERNAME = "username"
PASSWORD = mock.MagicMock()
QUEUE_NAME = "scheduled-jobs"
FAILURE_QUEUE_NAME = "failed-scheduled-jobs"


@pytest.fixture
def setup_queue_consumer():
    message_callback = mock.MagicMock()
    ingress_connection = mock.MagicMock()
    ingress_channel = mock.MagicMock()
    ingress_connection.channel.return_value = ingress_channel
    failure_connection = mock.MagicMock()
    failure_channel = mock.MagicMock()
    failure_connection.channel.return_value = failure_channel

    with (
        mock.patch("jobcreator.queue_consumer.ConnectionParameters") as connection_parameters,
        mock.patch("jobcreator.queue_consumer.BlockingConnection") as blocking_connection,
    ):
        blocking_connection.side_effect = [ingress_connection, failure_connection]
        queue_consumer = QueueConsumer(
            message_callback,
            QUEUE_HOST,
            USERNAME,
            PASSWORD,
            QUEUE_NAME,
            FAILURE_QUEUE_NAME,
        )
        yield SimpleNamespace(
            queue_consumer=queue_consumer,
            blocking_connection=blocking_connection,
            connection_parameters=connection_parameters,
            message_callback=message_callback,
            ingress_connection=ingress_connection,
            ingress_channel=ingress_channel,
            failure_connection=failure_connection,
            failure_channel=failure_channel,
        )


def reset_runtime_mocks(setup_queue_consumer):
    setup_queue_consumer.message_callback.reset_mock()
    setup_queue_consumer.ingress_channel.reset_mock()
    setup_queue_consumer.failure_channel.reset_mock()


def test_init_creates_credentials_and_connection_parameters(setup_queue_consumer):
    setup = setup_queue_consumer
    queue_consumer = setup.queue_consumer

    assert queue_consumer.message_callback == setup.message_callback
    assert queue_consumer.queue_host == QUEUE_HOST
    assert queue_consumer.queue_name == QUEUE_NAME
    assert queue_consumer.failure_queue_name == FAILURE_QUEUE_NAME

    credentials = PlainCredentials(username=USERNAME, password=PASSWORD)
    setup.connection_parameters.assert_called_once_with(QUEUE_HOST, 5672, credentials=credentials)
    assert setup.connection_parameters.return_value == queue_consumer.connection_parameters

    setup.blocking_connection.assert_has_calls(
        [
            mock.call(queue_consumer.connection_parameters),
            mock.call(queue_consumer.connection_parameters),
        ]
    )
    assert setup.ingress_connection == queue_consumer.connection
    assert setup.ingress_channel == queue_consumer.channel
    assert setup.failure_connection == queue_consumer.failure_connection
    assert setup.failure_channel == queue_consumer.failure_channel


def test_connect_to_broker_declares_ingress_and_failure_queues(setup_queue_consumer):
    setup = setup_queue_consumer

    setup.ingress_channel.exchange_declare.assert_called_once_with(QUEUE_NAME, exchange_type="direct", durable=True)
    setup.ingress_channel.queue_declare.assert_called_once_with(
        QUEUE_NAME,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    setup.ingress_channel.queue_bind.assert_called_once_with(QUEUE_NAME, QUEUE_NAME, routing_key="")
    setup.ingress_channel.basic_qos.assert_called_once_with(prefetch_count=1)

    setup.failure_channel.exchange_declare.assert_called_once_with(
        FAILURE_QUEUE_NAME,
        exchange_type="direct",
        durable=True,
    )
    setup.failure_channel.queue_declare.assert_called_once_with(
        FAILURE_QUEUE_NAME,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    setup.failure_channel.queue_bind.assert_called_once_with(
        FAILURE_QUEUE_NAME,
        FAILURE_QUEUE_NAME,
        routing_key="",
    )
    setup.failure_channel.basic_qos.assert_called_once_with(prefetch_count=1)


def test_message_handler(setup_queue_consumer):
    setup = setup_queue_consumer
    message = '{"help": "im stuck"}'
    msg_obj = {"help": "im stuck"}
    setup.message_callback.reset_mock()

    with mock.patch("jobcreator.queue_consumer.logger") as logger:
        setup.queue_consumer._message_handler(message)

    logger.info.assert_called_once_with("Message decoded as: %s", msg_obj)
    setup.message_callback.assert_called_once_with(msg_obj)


def test_message_handler_on_json_decode_error_raises(setup_queue_consumer):
    setup = setup_queue_consumer
    message = "{}::::::::://1//1/1!!!''''''"
    setup.message_callback.reset_mock()

    with mock.patch("jobcreator.queue_consumer.logger") as logger, pytest.raises(json.JSONDecodeError):
        setup.queue_consumer._message_handler(message)

    logger.error.assert_called_once_with(
        "Error attempting to decode JSON: %s",
        "Extra data: line 1 column 3 (char 2)",
    )
    setup.message_callback.assert_not_called()


def test_message_handler_callback_exception_raises_after_logging(setup_queue_consumer):
    setup = setup_queue_consumer
    setup.message_callback.side_effect = RuntimeError("callback failed")

    with mock.patch("jobcreator.queue_consumer.logger") as logger, pytest.raises(RuntimeError, match="callback failed"):
        setup.queue_consumer._message_handler('{"help": "im stuck"}')

    logger.exception.assert_called_once_with("Problem processing message callback")


def test_start_consuming_successful_ingress_acks_and_does_not_publish_failure(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    header = mock.MagicMock()
    header.delivery_tag = "ingress-tag"
    body = b'{"help": "im stuck"}'
    setup.ingress_channel.consume.return_value = [(header, None, body)]
    setup.failure_channel.consume.return_value = []
    callback = mock.MagicMock()

    with mock.patch("jobcreator.queue_consumer.time.sleep"):
        setup.queue_consumer.start_consuming(callback, run_once=True)

    callback.assert_called_once_with()
    setup.message_callback.assert_called_once_with({"help": "im stuck"})
    setup.ingress_channel.basic_ack.assert_called_once_with("ingress-tag")
    setup.failure_channel.basic_publish.assert_not_called()


def test_start_consuming_malformed_json_publishes_failure_before_ack(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    header = mock.MagicMock()
    header.delivery_tag = "ingress-tag"
    body = b"not-json"
    setup.ingress_channel.consume.return_value = [(header, None, body)]
    setup.failure_channel.consume.return_value = []
    call_order = mock.Mock()
    call_order.attach_mock(setup.failure_channel.basic_publish, "publish")
    call_order.attach_mock(setup.ingress_channel.basic_ack, "ack")

    with mock.patch("jobcreator.queue_consumer.time.sleep"):
        setup.queue_consumer.start_consuming(mock.MagicMock(), run_once=True)

    assert call_order.mock_calls == [
        mock.call.publish(FAILURE_QUEUE_NAME, "", body),
        mock.call.ack("ingress-tag"),
    ]


def test_start_consuming_callback_exception_publishes_failure_before_ack(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    setup.message_callback.side_effect = RuntimeError("callback failed")
    header = mock.MagicMock()
    header.delivery_tag = "ingress-tag"
    body = b'{"help": "im stuck"}'
    setup.ingress_channel.consume.return_value = [(header, None, body)]
    setup.failure_channel.consume.return_value = []
    call_order = mock.Mock()
    call_order.attach_mock(setup.failure_channel.basic_publish, "publish")
    call_order.attach_mock(setup.ingress_channel.basic_ack, "ack")

    with mock.patch("jobcreator.queue_consumer.time.sleep"):
        setup.queue_consumer.start_consuming(mock.MagicMock(), run_once=True)

    assert call_order.mock_calls == [
        mock.call.publish(FAILURE_QUEUE_NAME, "", body),
        mock.call.ack("ingress-tag"),
    ]


def test_start_consuming_failed_queue_success_acks_failed_message(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    header = mock.MagicMock()
    header.delivery_tag = "failed-tag"
    body = b'{"help": "im stuck"}'
    setup.ingress_channel.consume.return_value = []
    setup.failure_channel.consume.return_value = [(header, None, body)]

    with mock.patch("jobcreator.queue_consumer.time.sleep"):
        setup.queue_consumer.start_consuming(mock.MagicMock(), run_once=True)

    setup.message_callback.assert_called_once_with({"help": "im stuck"})
    setup.failure_channel.basic_ack.assert_called_once_with("failed-tag")
    setup.failure_channel.basic_publish.assert_not_called()


def test_start_consuming_failed_queue_failure_republishes_before_ack(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    setup.message_callback.side_effect = RuntimeError("callback failed")
    header = mock.MagicMock()
    header.delivery_tag = "failed-tag"
    body = b'{"help": "im stuck"}'
    setup.ingress_channel.consume.return_value = []
    setup.failure_channel.consume.return_value = [(header, None, body)]
    call_order = mock.Mock()
    call_order.attach_mock(setup.failure_channel.basic_publish, "publish")
    call_order.attach_mock(setup.failure_channel.basic_ack, "ack")

    with mock.patch("jobcreator.queue_consumer.time.sleep"):
        setup.queue_consumer.start_consuming(mock.MagicMock(), run_once=True)

    assert call_order.mock_calls == [
        mock.call.publish(FAILURE_QUEUE_NAME, "", body),
        mock.call.ack("failed-tag"),
    ]


def test_start_consuming_publish_failure_does_not_ack_source_message(setup_queue_consumer):
    setup = setup_queue_consumer
    reset_runtime_mocks(setup)
    setup.message_callback.side_effect = RuntimeError("callback failed")
    setup.failure_channel.basic_publish.side_effect = RuntimeError("publish failed")
    header = mock.MagicMock()
    header.delivery_tag = "ingress-tag"
    body = b'{"help": "im stuck"}'
    setup.ingress_channel.consume.return_value = [(header, None, body)]

    with pytest.raises(RuntimeError, match="publish failed"):
        setup.queue_consumer.start_consuming(mock.MagicMock(), run_once=True)

    setup.ingress_channel.basic_ack.assert_not_called()
