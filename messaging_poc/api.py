# SPDX-License-Identifier: GPL-3.0-or-later
"""The API for publishing messages and consuming from message queues."""
from __future__ import absolute_import, unicode_literals
import inspect
import logging

import proton.reactor

from messaging_poc import consumer, exceptions


log = logging.getLogger(__name__)


def _check_callback(callback):
    """
    Turns a callback that is potentially a class into a callable object.

    Args:
        callback (object): An object that might be a class, method, or function.
        if the object is a class, this creates an instance of it.

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
            raise ValueError(
                "Callback must be a class that implements __call__ or a function."
            )
    elif callable(callback):
        callback_object = callback
    else:
        raise ValueError(
            "Callback must be a class that implements __call__ or a function."
        )

    return callback_object


# TODO: fedora-messaging uses the bindings and queues kwargs
def consume(callback):
    # TODO: refactor docstring
    """
    Start a message consumer that executes the provided callback when messages are
    received.

    This API is blocking and will not return until the process receives a signal
    from the operating system.

    The callback receives a single positional argument, the message:

    >>> from messaging_poc import api
    >>> def my_callback(message):
    ...     print(message)
    >>> api.consume(my_callback)

    :param callable callback: A callable object that accepts one positional argument,
        a :class:`Message` or a class object that implements the ``__call__``
        method. The class will be instantiated before use.

    :raises messaging_poc.exceptions.HaltConsumer: If the consumer requests that it be stopped.
    :raises ValueError: If the consumer provides a callback that is not a class that
        implements __call__ and is not a function.
    """
    callback = _check_callback(callback)

    try:
        proton.reactor.Container(consumer.Consumer(callback)).run()
    # TODO: Actually raise this after shutdown
    except (ValueError, exceptions.HaltConsumer):
        raise
    except Exception:
        # https://crochet.readthedocs.io/en/stable/workarounds.html#missing-tracebacks
        log.error("Consuming raised an unexpected error, please report a bug.")
        raise
