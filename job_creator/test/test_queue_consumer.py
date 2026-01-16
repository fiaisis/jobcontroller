from unittest import mock

import pytest
from pika import PlainCredentials

from jobcreator.queue_consumer import QueueConsumer

MESSAGE_CALLBACK = mock.MagicMock()
QUEUE_HOST = mock.MagicMock()
USERNAME = mock.MagicMock()
PASSWORD = mock.MagicMock()
QUEUE_NAME = mock.MagicMock()


@pytest.fixture(autouse=True, scope="module")
def setup_queue_consumer():
    with (
        mock.patch("jobcreator.queue_consumer.ConnectionParameters") as connection_parameters,
        mock.patch("jobcreator.queue_consumer.BlockingConnection") as blocking_connection,
    ):
        quc = QueueConsumer(MESSAGE_CALLBACK, QUEUE_HOST, USERNAME, PASSWORD, QUEUE_NAME)
        return quc, blocking_connection, connection_parameters


def test_init_creates_credentials_and_connection_parameters(setup_queue_consumer):
    quc, blocking_connection, connection_parameters = setup_queue_consumer
    assert quc.message_callback == MESSAGE_CALLBACK
    assert quc.queue_host == QUEUE_HOST
    assert quc.queue_name == QUEUE_NAME

    credentials = PlainCredentials(username=USERNAME, password=PASSWORD)
    connection_parameters.assert_called_once_with(QUEUE_HOST, 5672, credentials=credentials)
    assert connection_parameters.return_value == quc.connection_parameters

    blocking_connection.assert_called_once_with(quc.connection_parameters)
    assert blocking_connection.return_value == quc.connection
    assert blocking_connection.return_value.channel.return_value == quc.channel


def test_connect_to_broker(setup_queue_consumer):
    quc, blocking_connection, _ = setup_queue_consumer
    blocking_connection.assert_called_once_with(quc.connection_parameters)
    assert blocking_connection.return_value == quc.connection
    channel = blocking_connection.return_value.channel.return_value
    assert channel == quc.channel

    channel.exchange_declare.assert_called_once_with(QUEUE_NAME, exchange_type="direct", durable=True)
    channel.queue_declare.assert_called_once_with(QUEUE_NAME, durable=True, arguments={"x-queue-type": "quorum"})
    channel.queue_bind.assert_called_once_with(QUEUE_NAME, QUEUE_NAME, routing_key="")


def test_message_handler(setup_queue_consumer):
    quc, _, _ = setup_queue_consumer
    message = '{"help": "im stuck"}'
    msg_obj = {"help": "im stuck"}
    MESSAGE_CALLBACK.reset_mock()

    with mock.patch("jobcreator.queue_consumer.logger") as logger:
        quc._message_handler(message)

    logger.info.assert_called_once_with("Message decoded as: %s", msg_obj)
    MESSAGE_CALLBACK.assert_called_once_with(msg_obj)


def test_message_handler_on_json_decode_error(setup_queue_consumer):
    quc, _, _ = setup_queue_consumer
    message = "{}::::::::://1//1/1!!!''''''"
    MESSAGE_CALLBACK.reset_mock()

    with mock.patch("jobcreator.queue_consumer.logger") as logger:
        quc._message_handler(message)

    logger.error.assert_called_once_with(
        "Error attempting to decode JSON: %s",
        "Extra data: line 1 column 3 (char 2)",
    )
    MESSAGE_CALLBACK.assert_not_called()


def test_start_consuming(setup_queue_consumer):
    quc, _, _ = setup_queue_consumer
    quc._message_handler = mock.MagicMock()
    header = mock.MagicMock()
    body = mock.MagicMock()
    quc.channel.consume.return_value = [(header, mock.MagicMock(), body)]
    callback = mock.MagicMock()

    quc.start_consuming(callback, run_once=True)

    callback.assert_called_once_with()
    quc.channel.consume.assert_called_once_with(quc.queue_name, inactivity_timeout=5)
    quc._message_handler.assert_called_once_with(body.decode.return_value)
    quc.channel.basic_ack.assert_called_once_with(header.delivery_tag)


def test_start_consumer_will_handle_exceptions_as_warnings(setup_queue_consumer):
    quc, _, _ = setup_queue_consumer

    def raise_exception():
        raise Exception("The worst exception!")

    quc._message_handler = mock.MagicMock(side_effect=raise_exception)
    body = mock.MagicMock()
    quc.channel.consume.return_value = [(mock.MagicMock(), mock.MagicMock(), body)]

    with mock.patch("jobcreator.queue_consumer.logger") as logger:
        quc.start_consuming(mock.MagicMock(), run_once=True)

    logger.warning.assert_called_once_with("Problem processing message: %s", body)
