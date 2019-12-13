# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import absolute_import, unicode_literals
import datetime
import json
import logging
import uuid

import jsonschema

from messaging_poc.exceptions import ValidationError

#: Indicates the message is for debugging or is otherwise very low priority.
DEBUG = 10

#: Indicates the message is informational.
INFO = 20

#: Indicates a problem or an otherwise important problem.
WARNING = 30

#: Indicates a critically important message that users should act upon as soon as
#: possible.
ERROR = 40

#: A tuple of all valid severity levels
SEVERITIES = (DEBUG, INFO, WARNING, ERROR)

_log = logging.getLogger(__name__)


def get_message(message):
    """
    Convert a :class:`proton.Message` object to a :class:`Message` object.

    Args:
        message (proton.Message): The proton Message object to convert.

    Raises:
        ValidationError: If the message is invalid and can't be converted.

    Returns:
        Message: A Message object.
    """
    body = message.body
    message_id = message.id
    _log.debug(
        "Converting Message (ID=%r, headers=%r, body=%r) to a Message object",
        message_id,
        message.properties,
        body,
    )

    if message.properties is None:
        _log.debug("Message (ID=%r) arrived without headers.", message_id)
        message.properties = {}

    if "fedora_messaging_schema" in message.properties:
        _log.warning(
            "Message (ID=%r) arrived with a schema header. "
            "This is not supported in this library and it will be ignored.",
            message_id,
        )

    try:
        severity = message.properties["fedora_messaging_severity"]
    except KeyError:
        _log.debug("Message (ID=%r) arrived without a severity.", message_id)
        severity = INFO

    content_encoding = message.content_encoding
    # Note that the quotes around None are intentional. The value for message.content_encoding
    # is a class that is derived from the str class, and if there is no content encoding, then
    # qpid-proton uses a value equivalent to "None".
    if content_encoding == "None":
        _log.debug("Message arrived without a content encoding. Assuming utf-8.")
        content_encoding = "utf-8"

    # For some reason, some of the message bodies are decoded while others are not
    if isinstance(message.body, bytes):
        try:
            body = body.decode(content_encoding)
        except UnicodeDecodeError as e:
            _log.error(
                "Unable to decode message body %r with %s content encoding", body, content_encoding
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
            user_id = message.user_id.decode("utf-8")
        except UnicodeDecodeError:
            _log.warning("Unable to decode the user-id %r message property", message.user_id)

    # Some of these properties are not exposed to the user and are thus unused, but when
    # instantiating a Message class to publish a message, these properties get used.
    # This just sets them for consistency.
    properties = {
        # Since the message was loaded as JSON, just set the appropriate content_type regardless
        # of the message's actual content type.
        "content_type": "application/json",
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
    Messages are simply JSON-encoded objects. This allows message authors to
    define a schema and implement Python methods to abstract the raw message
    from the user. This allows the schema to change and evolve without breaking
    the user-facing API.

    There are a number of properties that are intended to be overridden by
    users. These fields are used to sort messages for notifications or are
    used to create human-readable versions of the messages. Properties that are
    intended for this purpose are noted in their attribute documentation below.

    Please note that custom schemas are not yet supported by this implementation
    like they are fedora-messaging.

    Args:
        headers (dict): A set of message headers. Consult the headers schema for
            expected keys and values. Please note that these are actually
            application properties. The name is kept to keep compatibility with
            fedora-messaging, which is AMQP 0.9 based.
        body (dict): The message body. Consult the body schema for expected keys
            and values. This dictionary must be JSON-serializable by the default
            serializer.
        topic (six.text_type): The message topic as a unicode string. If this is
            not provided, the default topic for the class is used. See the
            attribute documentation below for details.
        properties (dict): The AMQP message properties. If this is not provided, they
            will be generated. Most users should not need to provide this, but it can
            be useful in testing scenarios.
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.

    Attributes:
        id (six.text_type): The message id as a unicode string. This attribute is
            automatically generated and set by the library and users should only
            set it themselves in testing scenarios.
        topic (six.text_type): The message topic as a unicode string. The topic
            is used by message consumers to filter what messages they receive.
            Topics should be a string of words separated by '.' characters,
            with a length limit of 255 bytes. Because of this byte limit, it is
            best to avoid non-ASCII character. Topics should start general and
            get more specific each word. For example: "bodhi.update.kernel" is
            a possible topic. "bodhi" identifies the application, "update"
            identifies the message, and "kernel" identifies the package in the
            update. This can be set at a class level or on a instance level.
            Dynamic, specific topics that allow for fine-grain filtering are
            preferred.
        headers_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message headers. For
            most users, the default definition should suffice.
        body_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message body. The body_schema
            is retrieved on a message instance so it is not required to be a
            class attribute, although this is a convenient approach. Users are
            also free to write the JSON schema as a file and load the file from
            the filesystem or network if they prefer.
        body (dict): The message body as a Python dictionary. This is validated by
            the body schema before publishing and before consuming.
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.
        queue (str): The name of the queue this message arrived through. This
            attribute is set automatically by the library and users should never
            set it themselves.
    """

    severity = INFO
    topic = ""
    headers_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message headers",
        "type": "object",
        "properties": {
            "fedora_messaging_severity": {"type": "number", "enum": [DEBUG, INFO, WARNING, ERROR]},
            "fedora_messaging_schema": {"type": "string"},
            "sent-at": {"type": "string"},
        },
    }
    body_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message body",
        "type": "object",
    }

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
                "id": str(uuid.uuid4()),
            }
        self.queue = None

    @property
    def id(self):
        """
        Get the message's ID.

        Returns:
            str: The message ID.
        """
        return self._properties.get("id")

    @id.setter
    def id(self, value):
        self._properties["id"] = value

    @property
    def user_id(self):
        """
        Get the user that published the message through user-id AMQP message property.

        Note that this diverges from the fedora-messaging Message class.

        Returns:
            str: The user-id AMQP message property.
        """
        return self._properties.get("user_id")

    def __repr__(self):
        """
        Provide a printable representation of the object.
        """
        return "<{} id={!r}, topic={!r}>".format(
            self.__class__.__name__, self.id, self.topic, self.body
        )

    def __eq__(self, other):
        """
        Two messages of the same class with the same topic, headers, and body are equal.

        The "sent-at" header is excluded from the equality check as this is set
        automatically and is dependent on when the object is created.

        Args:
            other (object): The object to check for equality.

        Returns:
            bool: True if the messages are equal.
        """
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
            _log.debug('Validating message headers "%r" with schema "%r"', self._headers, schema)
            jsonschema.validate(self._headers, schema)
        for schema in (self.body_schema, Message.body_schema):
            _log.debug('Validating message body "%r" with schema "%r"', self.body, schema)
            jsonschema.validate(self.body, schema)

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed, long-form representation of the
        message. The default implementation is to format the raw message ID,
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
