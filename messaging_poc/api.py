# SPDX-License-Identifier: GPL-3.0-or-later
"""The API for publishing messages and consuming from message queues."""
from __future__ import absolute_import, unicode_literals
import inspect
import logging

import proton.reactor

from messaging_poc import config, consumer, exceptions
from messaging_poc.message import Message, SEVERITIES

__all__ = ("consume", "Message", "SEVERITIES")

log = logging.getLogger(__name__)


def _check_callback(callback):
    """
    Turns a callback that is potentially a class into a callable object.

    Args:
        callback (object): An object that might be a class, method, or function.
            If the object is a class, this creates an instance of it.

    Raises:
        ValueError: If an instance can't be created or it isn't a callable object.
        TypeError: If the class requires arguments to be instantiated.

    Returns:
        callable: A callable object suitable for use as the consumer callback.
    """
    # If the callback is a class, create an instance of it first
    if inspect.isclass(callback):
        callback_object = callback()
        if not callable(callback_object):
            raise ValueError("Callback must be a class that implements __call__ or a function.")
    elif callable(callback):
        callback_object = callback
    else:
        raise ValueError(
            "Callback must be a class/instance that implements __call__ or a function."
        )

    return callback_object


def consume(callback, virtual_topics=None):
    """
    Start a message consumer that executes the provided callback when messages are received.

    This API is blocking and will not return until the process receives a signal
    from the operating system.

    The callback runs in a separate thread so it doesn't block the consumer heartbeat
    frames with the broker.

    The callback receives a single positional argument, the message:

    >>> from messaging_poc import api
    >>> def my_callback(message):
    ...     print(message)
    >>> virtual_topics = [
    ...     {
    ...         "durable": True,
    ...         "topic: "eng.some_other_service.>",
    ...         "queue_name": "app-prod",
    ...     },
    ... ]
    >>> api.consume(my_callback, virtual_topics=virtual_topics)

    If the virtual_topics argument is not provided, it will be loaded from the configuration.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message` or a class object that implements the ``__call__``
            method. The class will be instantiated before use.
        virtual_topics (dict or list of dict): virtual topics to declare before consuming.
            This should be the same format as the :ref:`conf-virtual_topics` configuration.
            If this is not provided, the value in the configuration will be used.

    Raises:
        messaging_poc.exceptions.HaltConsumer: If the consumer requests that
            it be stopped.
        ValueError: If the consumer provides a callback that is not a class that
            implements __call__ and is not a function, or if the virtual_topics argument
            is not a dict or list of dicts with the proper keys.
    """
    if isinstance(virtual_topics, dict):
        virtual_topics = [virtual_topics]

    if virtual_topics is None:
        virtual_topics = config.conf["virtual_topics"]
    else:
        try:
            config.validate_virtual_topics(virtual_topics)
        except exceptions.ConfigurationException as e:
            raise ValueError(e.message)

    callback = _check_callback(callback)
    amqp_urls = [url.strip() for url in config.conf["amqp_url"].split(",")]

    proton.reactor.Container(
        consumer.Consumer(
            amqp_urls,
            virtual_topics,
            callback,
            config.conf["tls"],
            config.conf["heartbeat"],
        )
    ).run()
