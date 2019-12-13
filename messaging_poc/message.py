# SPDX-License-Identifier: GPL-3.0-or-later
"""TODO."""
import datetime
import json
import logging
import uuid

import jsonschema

from messaging_poc.exceptions import ValidationError

#: Indicates the message is for debugging or is otherwise very low priority. Users
#: will not be notified unless they've explicitly requested DEBUG level messages.
DEBUG = 10

#: Indicates the message is informational. End users will not receive notifications
#: for these messages by default. For example, automated tests passed for their
#: package.
INFO = 20

#: Indicates a problem or an otherwise important problem. Users are notified of
#: these messages when they pertain to packages they are associated with by default.
#: For example, one or more automated tests failed against their package.
WARNING = 30

#: Indicates a critically important message that users should act upon as soon as
#: possible. For example, their package no longer builds.
ERROR = 40

#: A tuple of all valid severity levels
SEVERITIES = (DEBUG, INFO, WARNING, ERROR)

_log = logging.getLogger(__name__)


def get_message(message):
    """TODO"""
    body = message.body
    message_id = message.id

    if message.properties is None:
        _log.debug("Message (ID=%r, body=%r) arrived without headers.", message_id, body)
        message.properties = {}

    if "fedora_messaging_schema" in message.properties:
        _log.warning(
            "Message (ID=%r, headers=%r, body=%r) arrived with a schema header. "
            "This is not supported in this library and it will be ignored.",
            message_id,
            message.properties,
            body,
        )

    try:
        severity = message.properties["fedora_messaging_severity"]
    except KeyError:
        _log.debug("Message (ID=%r, body=%r) arrived without a severity.", message_id, body)
        severity = INFO

    if message.content_encoding == "None":
        _log.debug("Message arrived without a content encoding. Assuming utf-8.")
        message.content_encoding = "utf-8"

    # For some reason, some of the message bodies are decoded while others are not
    if isinstance(message.body, bytes):
        try:
            body = message.body.decode(message.content_encoding)
        except UnicodeDecodeError as e:
            _log.error(
                "Unable to decode message body %r with %s content encoding",
                body,
                message.content_encoding,
            )
            raise ValidationError(e)

    try:
        body = json.loads(body)
    except ValueError as e:
        _log.error("Failed to load message body %r, %r", body, e)
        raise ValidationError(e)

    topic = None

    if message.address.startswith("topic://"):
        # Omit the topic:// prefix
        topic = message.address[8:]

    user_id = None
    if message.user_id:
        try:
            user_id = message.user_id.decode('utf-8')
        except UnicodeDecodeError:
            _log.warning("Unable to decode the user-id message property")

    properties = {
        "content_type": message.content_type,
        "content_encoding": message.content_encoding,
        "durable": message.durable,
        "id": message.id,
        "user_id": user_id,
    }
    # headers is used to maintain compatibility with fedora-messaging, but these are actually
    # called application properties in AMQP 1.0.
    message_object = Message(
        body=body, properties=properties, headers=message.properties, topic=topic, severity=severity
    )

    try:
        message_object.validate()
        _log.debug("Successfully validated message %r", message_object)
    except jsonschema.exceptions.ValidationError as e:
        _log.error("Message validation of %r failed: %r", message_object, e)
        raise ValidationError(e)

    return message_object


class Message(object):
    """
    TODO
    """

    severity = INFO
    topic = ""
    headers_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message headers",
        "type": "object",
        "properties": {
            "fedora_messaging_severity": {
                "type": "number",
                "enum": [DEBUG, INFO, WARNING, ERROR],
            },
            "fedora_messaging_schema": {"type": "string"},
            "sent-at": {"type": "string"},
        },
    }
    body_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message body",
        "type": "object",
    }

    # TODO: Document that headers are actually application properties
    def __init__(self, body=None, headers=None, topic=None, properties=None, severity=None):
        self.body = body or {}

        if topic:
            # Default is "" on base class
            self.topic = topic

        if severity:
            self.severity = severity

        # These are actually application properties in AMQP 1.0. The term headers is used to
        # maintain compatibility with fedora-messaging.
        self._headers = headers or {}
        now = datetime.datetime.utcnow().replace(microsecond=0, tzinfo=datetime.timezone.utc)
        # TODO: fedora-messaging also has the fedora_messaging_schema key, but this library doesn't
        # support schema plugins yet.
        self._headers.update(
            {"sent-at": now.isoformat(), "fedora_messaging_severity": self.severity}
        )

        if properties:
            self._properties = properties
        else:
            # These map to keyword arguments for the proton.Message constructor
            self._properties = {
                "content_type": "application/json",
                "content_encoding": "utf-8",
                # TODO: Verify this is needed
                # "durable": True,
                "id": str(uuid.uuid4()),
            }
        self.queue = None

    @property
    def id(self):
        return self._properties.get("id")

    @id.setter
    def id(self, value):
        self._properties["id"] = value

    @property
    def user_id(self):
        # TODO: Document that this is not part of the fedora-messaging base class
        return self._properties.get("user_id")

    def __repr__(self):
        """
        Provide a printable representation of the object.
        """
        return "<{} id={!r}, topic={!r}>".format(
            self.__class__.__name__, self.id, self.topic, self.body
        )

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        headers = self._headers.copy()
        other_headers = other._headers.copy()
        try:
            del headers["sent-at"]
        except KeyError:
            pass
        try:
            del other_headers["sent-at"]
        except KeyError:
            pass

        return self.topic == other.topic and self.body == other.body and headers == other_headers

    def validate(self):
        """
        Validate the headers and body with the message schema, if any.

        In addition to the user-provided schema, all messages are checked against
        the base schema which requires certain message headers and the that body
        be a JSON object.

        .. warning:: This method should not be overridden by sub-classes.

        Raises:
            jsonschema.ValidationError: If either the message headers or the message body
                are invalid.
            jsonschema.SchemaError: If either the message header schema or the message body
                schema are invalid.
        """
        for schema in (self.headers_schema, Message.headers_schema):
            _log.debug(
                'Validating message headers "%r" with schema "%r"',
                self._headers,
                schema,
            )
            jsonschema.validate(self._headers, schema)
        for schema in (self.body_schema, Message.body_schema):
            _log.debug(
                'Validating message body "%r" with schema "%r"', self.body, schema
            )
            jsonschema.validate(self.body, schema)

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed, long-form representation of the
        message. The default implementation is to format the raw message id,
        topic, headers, and body.

        .. note:: Sub-classes should override this method. It is used to create
            the body of email notifications and by other tools to display messages
            to humans.
        """
        return "Id: {i}\nTopic: {t}\nHeaders: {h}\nBody: {b}".format(
            i=self.id,
            t=self.topic,
            h=json.dumps(self._headers, sort_keys=True, indent=4, separators=(",", ": ")),
            b=json.dumps(self.body, sort_keys=True, indent=4, separators=(",", ": ")),
        )


def dumps(messages):
    # TODO
    pass


def loads(serialized_messages):
    # TODO
    pass
