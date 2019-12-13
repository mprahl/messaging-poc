# SPDX-License-Identifier: GPL-3.0-or-later
"""
messaging_poc can be configured with the
``/etc/messaging-poc/config.toml`` file or by setting the
``MESSAGING_POC_CONF`` or ``FEDORA_MESSAGING_CONF`` environment variable
to the path of the configuration file.

Each configuration option has a default value.

.. contents:: Table of Configuration Options
    :local:

A complete example TOML configuration:

.. literalinclude:: ../config.toml.example


Generic Options
===============

These options apply to both consumers and publishers.

.. _conf-amqp-url:

amqp_url
--------
The AMQP broker to connect to. This URL should be in the format
of ``<protocol>://<server>:<port>``. More than one URL can be supplied
by separating them with commas. This defaults to ``'amqp://``.

.. _conf-tls:

tls
---
A dictionary of the TLS settings to use for client authentication when
connecting to the AMQP broker. The default is::

    {
        'ca_cert': '/etc/pki/tls/certs/ca-bundle.crt',
        'certfile': None,
        'client_name': 'example-client',
        'keyfile': None,
    }

The value of ``ca_cert`` should be the path to a bundle of CA certificates used
to validate the certificate presented by the server. The 'keyfile' and
'certfile' values should be to the client key and client certificate to use
when authenticating with the broker. The 'client_name' field is an optional
field specifying the client name as defined in the certificate. If it's provided,
it will be used to construct the virtual topics to consume.

.. note:: The broker URL must use the ``amqps`` scheme.


.. _conf-log-config:

log_config
----------
A dictionary describing the logging configuration to use, in a format accepted
by :func:`logging.config.dictConfig`.

.. note:: Logging is only configured for consumers, not for producers.


Consumer Options
================

The following configuration options are consumer-related.

.. _conf-heartbeat:

The number of seconds to use for the heartbeat. This defaults to 60 seconds.
Please note that the underlying library advertises half this value to the broker,
and then tries to send a heartbeat frame every half of this value in seconds.

.. _conf-virtual_topics:

virtual_topics
------
An array of dictionaries describing the virtual topics to create queues for on the
broker. The keys are:

* ``durable`` - whether or not the queue should survive a broker restart.
  This is optional and defaults to ``False``.

* ``topic`` - the topic the queue should listen on (e.g. ``eng.greenwave.>``).

* ``queue_name`` - the name of the queue. If more than one consumer has the same
  ``client_name`` and ``queue_name``, the messages will be load-balanced.

For example::

    [
        {
            "durable": True,
            "topic: "eng.some_other_service.>",
            "queue_name": "app-prod",
        },
    ]

consumer_config
---------------
A dictionary for the consumer to use as configuration. The consumer should
access this key in its callback for any configuration it needs. Defaults to
an empty dictionary. If, for example, this dictionary contains the
``print_messages`` key, the callback can access this configuration with::

    from messaging_poc import config

    def callback(message):
        if config.conf["consumer_config"]["print_messages"]:
            print(message)

"""
from __future__ import absolute_import, unicode_literals
import copy
import logging
import logging.config
import os

import toml

from messaging_poc import exceptions


_log = logging.getLogger(__name__)

#: The default configuration settings for this library. This should not be
#: modified and should be copied with :func:`copy.deepcopy`.
#: Note that ``heartbeat`` and ``virtual_topics`` are not available in fedora-messaging.
DEFAULTS = dict(
    amqp_url="amqp://",
    consumer_config={},
    heartbeat=60,
    log_config={
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"simple": {"format": "[%(name)s %(levelname)s] %(message)s"}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            }
        },
        "loggers": {
            "messaging_poc": {"level": "INFO", "propagate": False, "handlers": ["console"]}
        },
        # The root logger configuration; this is a catch-all configuration
        # that applies to all log messages not handled by a different logger
        "root": {"level": "WARNING", "handlers": ["console"]},
    },
    tls={"ca_cert": "/etc/pki/tls/certs/ca-bundle.crt", "certfile": None, "keyfile": None},
    virtual_topics=[],
)


def validate_virtual_topics(virtual_topics):
    """
    Validate the virtual_topics configuration.

    Raises:
        exceptions.ConfigurationException: If the configuration provided is of an
            invalid format.
    """
    if not isinstance(virtual_topics, (list, tuple)):
        raise exceptions.ConfigurationException(
            "virtual_topics must be a list or tuple of dictionaries, but was a {}".format(
                type(virtual_topics).__name__
            )
        )

    if not virtual_topics:
        raise exceptions.ConfigurationException("virtual_topics cannot be empty")

    for virtual_topic in virtual_topics:
        required_keys = {"client_name", "topic", "queue_name"}
        missing_keys = required_keys - set(virtual_topic.keys())
        if missing_keys:
            raise exceptions.ConfigurationException(
                "a virtual_topic is missing the following keys from its settings "
                "value: {}".format(sorted(missing_keys))
            )


class LazyConfig(dict):
    """This class lazy-loads the configuration file."""

    loaded = False

    def __getitem__(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).__getitem__(*args, **kw)

    def get(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).get(*args, **kw)

    def pop(self, *args, **kw):
        raise exceptions.ConfigurationException("Configuration keys cannot be removed!")

    def copy(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).copy(*args, **kw)

    def update(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).update(*args, **kw)

    def setup_logging(self):
        if not self.loaded:
            self.load_config()
        logging.config.dictConfig(self["log_config"])

    def _validate(self):
        """
        Perform checks on the configuration to assert its validity

        Raises:
            ConfigurationException: If the configuration is invalid.
        """
        for key in self:
            if key not in DEFAULTS:
                raise exceptions.ConfigurationException(
                    'Unknown configuration key "{}"! Valid configuration keys are'
                    " {}".format(key, list(DEFAULTS.keys()))
                )

        if not isinstance(self["heartbeat"], int):
            raise exceptions.ConfigurationException("The heartbeat must be an integer")

        validate_virtual_topics(self["virtual_topics"])

    def load_config(self, config_path=None):
        """
        Load application configuration from a file and merge it with the default
        configuration.

        If the ``MESSAGING_POC_CONF`` or ``FEDORA_MESSAGING_CONF`` environment variable
        is set to a filesystem path, the configuration will be loaded from that location.
        Otherwise, the path defaults to ``/etc/messaging-poc/config.toml``.
        """
        self.loaded = True
        config = copy.deepcopy(DEFAULTS)

        if config_path is None:
            if "MESSAGING_POC_CONF" in os.environ:
                config_path = os.environ["MESSAGING_POC_CONF"]
            elif "FEDORA_MESSAGING_CONF" in os.environ:
                config_path = os.environ["FEDORA_MESSAGING_CONF"]
            else:
                config_path = "/etc/messaging-poc/config.toml"

        if os.path.exists(config_path):
            _log.info("Loading configuration from {}".format(config_path))
            with open(config_path) as fd:
                try:
                    file_config = toml.load(fd)
                    for key in file_config:
                        config[key.lower()] = file_config[key]
                except toml.TomlDecodeError as e:
                    msg = "Failed to parse {}: error at line {}, column {}: {}".format(
                        config_path, e.lineno, e.colno, e.msg
                    )
                    raise exceptions.ConfigurationException(msg)
        else:
            _log.info("The configuration file, {}, does not exist.".format(config_path))

        self.update(config)
        self._validate()

        loggers = self["log_config"]["loggers"]
        if "fedora_messaging" in loggers:
            _log.warning(
                "Replacing the fedora_messaging logger with messaging_poc for compatibility"
            )
            loggers["messaging_poc"] = loggers["fedora_messaging"]
            loggers.pop("fedora_messaging")

        return self


#: The configuration dictionary used by this package and consumers.
conf = LazyConfig()
