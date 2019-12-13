# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import absolute_import, unicode_literals
import logging
import signal
import sys
import threading

import proton
import proton.reactor

from messaging_poc import exceptions
from messaging_poc.message import get_message

try:
    import Queue as queue
except ImportError:
    import queue


_log = logging.getLogger(__name__)


class MessageProcessor(object):
    """
    The message processor which runs the callback in a separate thread.

    Each callback runs serially in the order in which the messages were placed in the queue.

    Once the callback completes, a :class:`proton.reactor.ApplicationEvent` will be triggered
    so that the main thread properly settles the message based on how the callback requested.

    Args:
        callback (callable): The callback to call whenever there is a new message.
        injector (proton.reactor.EventInjector): The proton event injector to use when
            communicating with the main thread. This can't be created here because the
            injector is a selectable of the container.
    """

    #: A sentinel that indicates that the queue should be recreated. This is useful if the broker
    #: disconnects and there are still messages in the queue.
    flush_queue_sentinel = object()
    #: A sentinel that indicates that the processor thread should be stopped.
    shutdown_sentinel = object()

    def __init__(self, callback, injector):
        self.callback = callback
        self.injector = injector
        #: queue.PriorityQueue: The queue that the main thread will use to place messages to
        #: process.
        self._messages = queue.PriorityQueue()
        signal.signal(signal.SIGINT, self._signal_handler)
        # Run this in a separate thread to not block the main thread that maintains the connection
        # with the broker
        self.thread = threading.Thread(target=self._callback_wrapper)
        self.thread.daemon = True
        self.thread.start()

    def _callback_wrapper(self):
        """Run the callback for every new message."""
        while True:
            event = self._messages.get(block=True)
            if event is self.shutdown_sentinel:
                _log.debug("The shutdown sentinel was received, stopping the processor thread")
                break
            elif event is self.flush_queue_sentinel:
                _log.debug("The flush queue sentinel was received, recreating the message queue")
                self._messages = queue.PriorityQueue()
                continue

            _log.debug("Message id %s arrived from %s", event.message.id, event.message.address)
            subject = {
                "disposition_fields": {},
                "disposition_state": proton.Disposition.ACCEPTED,
                "exception": None,
            }
            try:
                message = get_message(event.message)
                # fedora-messaging messages have a queue property since it uses AMQP 0.9. Queues are
                # not part of the AMQP 1.0 spec, so the closest equivalent is the consumer's remote
                # source address. This is often a JMS queue if the broker is ActiveMQ.
                message.queue = event.receiver.remote_source.address
            except exceptions.ValidationError:
                _log.warning(
                    "Message id %s did not pass validation; ignoring message", event.message.id
                )
                subject["disposition_state"] = proton.Disposition.REJECTED
                self._respond(event, subject)
                continue

            try:
                _log.info(
                    "Consuming message from %s (message ID %s)",
                    event.message.address,
                    event.message.id,
                )
                self.callback(message)
            except exceptions.Nack:
                _log.warning("Returning message ID %s to the queue", message.id)
                subject["disposition_state"] = proton.Disposition.MODIFIED
                # This will set delivery-failed in the disposition frame
                subject["disposition_fields"] = {"failed": True}
            except exceptions.Drop:
                _log.warning("Consumer requested message ID %s be dropped", message.id)
                subject["disposition_state"] = proton.Disposition.REJECTED
            except exceptions.HaltConsumer as e:
                self.stop_thread()
                _log.info("Consumer indicated it wishes consumption to halt, shutting down")
                subject["exception"] = sys.exc_info()
                if e.requeue:
                    subject["disposition_state"] = proton.Disposition.RELEASED
            except Exception:
                self.stop_thread()
                _log.exception("Received unexpected exception from the callback, shutting down")
                subject["disposition_state"] = proton.Disposition.MODIFIED
                # This will set delivery-failed in the disposition frame
                subject["disposition_fields"] = {"failed": True}
                subject["exception"] = sys.exc_info()
            else:
                _log.info(
                    "Successfully consumed message from topic %s (message ID %s)",
                    message.topic,
                    message.id,
                )
            finally:
                self._respond(event, subject)

        _log.debug("The processing thread shutdown")
        # Tell the main thread that the processor thread has shutdown
        app_event = proton.reactor.ApplicationEvent("processor_shutdown")
        self.injector.trigger(app_event)
        self.injector = None

    def _respond(self, event, subject):
        """
        Tell the main thread to settle the message with the input disposition.

        Args:
            event: A proton new message event.
            subject (dict): A dictionary representing the disposition frame to send from the
                main thread to the broker and an optional exception to raise. The keys
                are disposition_fields, disposition_type, and exception.
        """
        # Attach the disposition information to the application event. This allows
        # on_callback_complete to respond accordingly to the broker. The response to the broker
        # needs to be in the main thread since it's not a thread-safe operation.
        app_event = proton.reactor.ApplicationEvent(
            "callback_complete", delivery=event.delivery, subject=subject
        )
        # Trigger this event on the main thread. This will call on_callback_complete.
        self.injector.trigger(app_event)

    def _signal_handler(self, signum, *args):
        """
        Gracefully stop the thread in response to a signal.

        Args:
            signum (signal.Signals): The signal that triggered the handler.
        """
        _log.debug("Received the signal %s", signum)
        self.stop_thread()

    def flush_queue(self):
        """
        Flush the message queue.

        This is useful for when the prefetch is greater than 1 and the broker reconnects. In this
        situation, the broker will send all the messages currently in the queue again since they
        have not been settled by the consumer.

        Note that if the callback is currently running, then the queue will be flushed immediately
        after the callback completes.
        """
        self._messages.put((0, self.flush_queue_sentinel))

    def process_message(self, event):
        """
        Place a message in the queue to be processed by the callback.

        Args:
            event: A proton new message event.
        """
        self._messages.put((1, event))

    def stop_thread(self):
        """Tell the thread to stop when the current callback completes."""
        _log.info("Stopping the processor thread")
        self.queue.put((0, self.shutdown_sentinel))
