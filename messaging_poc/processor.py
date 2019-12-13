# SPDX-License-Identifier: GPL-3.0-or-later
"""TODO."""
import logging
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
    def __init__(self, callback, injector):
        self.callback = callback
        self.injector = injector
        self.running = True
        # Messages will not be acked until after the callback completes
        self.messages = queue.Queue()
        # Run this in a separate thread to not block the main thread that maintains the connection
        # with the broker
        self.thread = threading.Thread(target=self._callback_wrapper)
        self.thread.daemon = True
        self.thread.start()

    def close(self):
        self.running = False

    def process_message(self, event):
        """TODO"""
        self.messages.put(event)

    def _respond(self, event, disposition_type):
        """TODO"""
        # Attach the disposition type to the application event. This allows on_callback_complete
        # to response properly to the broker. The response to the broker needs to be in the main
        # thread since it's not a thread safe operation.
        app_event = proton.reactor.ApplicationEvent(
            "callback_complete", delivery=event.delivery, subject=disposition_type
        )
        # Trigger this event on the main thread. This will call on_callback_complete.
        self.injector.trigger(app_event)

    def _callback_wrapper(self):
        while self.running:
            try:
                # Block at most 5 seconds in case there is a shutdown event in the meantime
                # TODO: Would it be cleaner to just pass a kill message instead of a timeout?
                event = self.messages.get(block=True, timeout=5)
            except queue.Empty:
                continue

            _log.debug("Message id %s arrived from %s", event.message.id, event.message.address)
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
                self._respond(event, proton.Disposition.REJECTED)

            shutdown = False
            disposition_type = proton.Disposition.ACCEPTED
            try:
                _log.info(
                    "Consuming message from %s (message id %s)",
                    event.message.address,
                    event.message.id,
                )
                self.callback(message)
            except exceptions.Nack:
                _log.warning("Returning message id %s to the queue", message.id)
                disposition_type = proton.Disposition.RELEASED
            except exceptions.Drop:
                _log.warning("Consumer requested message id %s be dropped", message.id)
                disposition_type = proton.Disposition.REJECTED
            except exceptions.HaltConsumer as e:
                shutdown = True
                _log.info("Consumer indicated it wishes consumption to halt, shutting down")
                if e.requeue:
                    disposition_type = proton.Disposition.RELEASED
            except Exception:
                shutdown = True
                _log.exception("Received unexpected exception from the callback, shutting down")
                disposition_type = proton.Disposition.RELEASED
            else:
                _log.info(
                    "Successfully consumed message from topic %s (message id %s)",
                    message.topic,
                    message.id,
                )
            finally:
                self._respond(event, disposition_type)

            if shutdown:
                # TODO: Clean shutdown
                _log.debug("Stopping the processing thread")
                break

        print("Closing thread")
        self.injector.close()
