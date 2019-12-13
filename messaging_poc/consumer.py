# SPDX-License-Identifier: GPL-3.0-or-later
from __future__ import absolute_import, unicode_literals
import logging

import proton
import proton.handlers
import proton.reactor
import six

from messaging_poc import exceptions
from messaging_poc.processor import MessageProcessor

_log = logging.getLogger(__name__)


class Consumer(proton.handlers.MessagingHandler):
    """
    The entrypoint to the qpid-proton reactor for a consumer.

    This consumer will automatically failover/reconnect on connection failures.

    Args:
        amqp_urls (list of str): The AMQP(S) URLs to use to connect to the broker
            in the format of <protocol>://<server>:<port>.
        virtual_topics (list of dict): The list of dictionaries describing the virtual topics
            to connect to. The required keys are topic and queue_name. You can also specify the
            durable key.
        callback (callable): The callback function to execute when a message is received.
        tls (dict): The TLS configuration settings used for client authentication. The expected
            keys are ca_cert, certfile, client_name (optional), and keyfile. If they are not
            specified, client authentication will be skipped.
        heartbeat (int, optional): The number of seconds to use for the heartbeat. This defaults
            to 60 seconds. Please note that the underlying qpid-proton library advertises half
            this value to the broker (see pni_process_conn_setup), and then tries to send a
            heartbeat frame every half of this value in seconds (see pn_tick_amqp).
    """

    def __init__(self, amqp_urls, virtual_topics, callback, tls=None, heartbeat=60):
        # auto_accept=False makes it so that messages can be individually accepted or rejected
        super(Consumer, self).__init__(prefetch=1, auto_accept=False)
        self.amqp_urls = amqp_urls
        self.callback = callback
        #: proton.Connection: The AMQP connection to the broker.
        self.connection = None
        #: proton.reactor.Container: The AMQP container. All selectables are associated with this
        #: object.
        self.container = None
        self.heartbeat = heartbeat
        #: list of str: The AMQP errors that cause the consumer to reconnect. These types of
        #: reconnections are handled in :meth:`handle_error` and not by qpid-proton.
        self.fatal_conditions.extend(
            ["amqp:decode-error", "amqp:invalid-field", "amqp:resource-limit-exceeded"]
        )
        #: Exception or tuple: A fatal exception that should be raised when
        #: :meth:`shutdown` is called. This can be a tuple from sys.exc_info().
        self.fatal_exception = None
        #: proton.reactor.EventInjector: The injector that is used to schedule events in the reactor
        #: from the main thread.
        self.injector = proton.reactor.EventInjector()
        #: messaging_poc.processor.MessageProcessor: The processor that runs the callback in a
        #: separate thread.
        self.processor = None
        #: dict: The proton.Receiver instances connected to the broker.
        self.receivers = {}
        self.tls = tls or {}
        self.virtual_topics = virtual_topics

    def _connect(self):
        """Create a connection to the broker from self.container."""
        _log.info("Connecting to the broker")
        # Note that the qpid-proton reconnect behavior is enabled by default. This reconnect occurs
        # when the underlying transport for the AMQP connection closes.
        self.container.connect(
            heartbeat=self.heartbeat, ssl_domain=self.ssl_domain, urls=self.amqp_urls
        )

    @staticmethod
    def _get_virtual_topic_queue(virtual_topic, client_name=None):
        """
        Convert a virtual topic dictionary to a queue.

        Args:
            virtual_topic (dict): The virtual topic dictionary to convert to.
            client_name (str): The optional client name from the TLS certificate to place in
                the virtual topic.

        Returns:
            str: The queue for the virtual topic.
        """
        client_suffix = ""
        if client_name:
            client_suffix = "."
        return "queue://Consumer.{}{}{}.VirtualTopic.{}".format(
            client_name or "", client_suffix, virtual_topic["queue_name"], virtual_topic["topic"]
        )

    @staticmethod
    def _is_durable(receiver):
        """
        Determine if a receiver is durable.

        Args:
            receiver (proton.Receiver): The receiver to check.

        Returns:
            bool: True if the receiver is durable.
        """
        return receiver.source and receiver.source.durability == proton.Terminus.DELIVERIES

    def disconnect(self):
        """
        Disconnect the AMQP container from the broker.

        If there are active receivers, they will be detached if the receiver is durable, and closed
        if not. If there is an active connection, it will be disconnected.
        """
        receivers = self.receivers or {}
        # Set self.receivers to an empty value so that on_link_remote_close doesn't reattach the
        # link
        self.receivers = {}
        for receiver in receivers.values():
            if not receiver.state & proton.Endpoint.LOCAL_ACTIVE:
                _log.debug(
                    "Skipping disconnecting the receiver to %s since it is not attached",
                    receiver.remote_source.address,
                )
                continue

            # Calling close() on a durable subscription ends the subscription, so use detach() when
            # it is durable
            if self._is_durable(receiver):
                _log.debug("Detaching the receiver to %s", receiver.remote_source.address)
                receiver.detach()
            else:
                _log.debug("Closing the receiver to %s", receiver.remote_source.address)
                receiver.close()

        if self.connection and self.connection.state & proton.Endpoint.LOCAL_ACTIVE:
            _log.info("Closing the connection to %s", self.connection.hostname)
            self.connection.close()
        else:
            _log.debug("Skipping closing the connection since it is not connected")
        self.connection = None

    def get_default_receiver_options(self):
        """
        Get the default receiver options when creating receivers.

        Returns:
            [list of proton.reactor.LinkOption]: A list of default options to pass to
            ``create_receiver``.
        """
        # This sets rcv-settle-mode to "first", which indicates that the Receiver MUST settle the
        # delivery once it has arrived without waiting for the Sender to settle first. This is all
        # ActiveMQ supports, so it would either default to this or negotiate this during link
        # attachment. This is explicitly set in the event this library is used with another broker.
        return [proton.reactor.AtLeastOnce()]

    def handle_error(self, event, error_type):
        """
        Handle an AMQP error.

        If the error is in self.fatal_conditions, it will cause a reconnect. If the error is due
        to a permission issue, a shutdown will be triggered.

        Args:
            event (any): A proton event of the AMQP error.
            error_type (str): The type of proton object.
        """
        endpoint = getattr(event, error_type, None)
        condition = getattr(endpoint, "remote_condition", None) or getattr(
            endpoint, "condition", None
        )
        if not condition:
            _log.error("An unspecified %s error occurred", error_type)

        _log.error("%s error of %s: %s", error_type, condition.name, condition.description)
        if condition.name == "amqp:unauthorized-access":
            self.processor.stop_thread()
            self.fatal_exception = exceptions.PermissionException(error_type, condition.description)
            self.shutdown()
        elif condition.name in self.fatal_conditions:
            self.disconnect()
            self._connect()

    def on_callback_complete(self, event):
        """
        Settle the message based on the callback result.

        The message can't be settled in :class:`.MessageProcessor` because it's in a different
        thread and this operation is not thread-safe. This method gets called by the processor
        thread triggering a :class:`proton.reactor.ApplicationEvent` of the type
        ``callback_complete``.

        Args:
            event (proton.reactor.ApplicationEvent): A proton application event.
        """
        if event.link.name not in self.receivers:
            _log.warning(
                "Skipping the disposition update because the receiver is no longer present"
            )
            return

        disposition_state = event.subject["disposition_state"]
        disposition_fields = event.subject.get("disposition_fields", {})
        _log.debug(
            "Responding with the disposition state %s and fields %r",
            disposition_state,
            disposition_fields,
        )
        event.delivery.update(disposition_state)
        for field, value in disposition_fields.items():
            setattr(event.delivery.local, field, value)

        # Since the "at least once" receive mode is used, the consumer must settle the message
        event.delivery.settle()
        # Once the message is settled, a credit is flowed to the broker signifying that the
        # consumer is ready for another message
        _log.debug("Flowing 1 link credit")
        event.delivery.link.flow(1)

        # When the callback triggers a shutdown through an exception, it is passed by the processor
        # thread so that it can be raised after the consumer's connection is closed
        exc = event.subject.get("exception")
        if exc:
            self.fatal_exception = exc

    def on_connection_closed(self, event):
        """
        Handle an AMQP connection that closed.

        Args:
            event: A proton connection closed event.
        """
        _log.info("The connection was closed")
        if self.processor:
            self.processor.flush_queue()
        self.connection = None

    def on_connection_error(self, event):
        """
        Handle an AMQP connection error.

        Args:
            event: A proton connection error event.
        """
        self.handle_error(event, "connection")

    def on_connection_opened(self, event):
        """
        Setup the processor and receivers (links) after the connection is opened.

        Args:
            event: A proton connection opened event.
        """
        _log.debug("The connection is open")
        self.connection = event.connection
        self.receivers = {}

        for virtual_topic in self.virtual_topics:
            source = self._get_virtual_topic_queue(virtual_topic, self.tls.get("client_name"))
            options = self.get_default_receiver_options()
            if virtual_topic.get("durable") is True:
                _log.debug("Starting a durable receiver on %s", source)
                options.append(proton.reactor.DurableSubscription())
            else:
                _log.debug("Starting a receiver on %s", source)
            receiver = self.container.create_receiver(self.connection, source, options=options)
            self.receivers[receiver.name] = receiver

    def on_link_closed(self, event):
        """
        Handle a link getting detached by the broker.

        If ActiveMQ encounters an error, it sometimes detaches the link (receiver) without an error.
        In this case, this method triggers a method to reattach the link.

        Args:
            event: A proton link closed event.
        """
        # If self.receivers is not set, then the link was detached/closed in an expected manner.
        if not self.receivers or event.link.name not in self.receivers:
            return

        _log.debug("The remote link %s unexpectedly closed", event.link.name)
        source = self.receivers[event.link.name].remote_source.address
        _log.warning("Reattaching the link for %s because it unexpectedly detached", source)
        options = self.get_default_receiver_options()
        # If the link was durable, recreate the receiver with the durable option
        if self._is_durable(self.receivers[event.link.name]):
            options.append(proton.reactor.DurableSubscription())
        receiver = self.container.create_receiver(self.connection, source, options=options)
        self.receivers.pop(event.link.name)
        self.receivers[receiver.name] = receiver

    def on_link_error(self, event):
        """
        Handle an AMQP link error.

        Args:
            event: A proton link error event.
        """
        self.handle_error(event, "link")

    def on_message(self, event):
        """
        Run the callback with the message in the processor thread.

        This method is non-blocking since the callback is run in the processor thread. This allows
        all other AMQP events to continue such as the heartbeat. When the processor is done
        processing the message, it will indirectly call :meth:`on_callback_complete` in the main
        thread.

        Args:
            event: A proton new message event.
        """
        _log.debug("Received a message with the delivery count %d", event.message.delivery_count)
        self.processor.process_message(event)

    def on_processor_shutdown(self, event):
        """
        Handle when the processor thread has shutdown.

        If the processor thread has shutdown, this means the consumer is being gracefully shutdown.
        This method gets called by the processor thread triggering a
        :class:`proton.reactor.ApplicationEvent` of the type ``processor_shutdown``.

        Args:
            event (proton.reactor.ApplicationEvent): A proton application event.
        """
        self.shutdown()

    def on_session_error(self, event):
        """
        Handle an AMQP session error.

        Args:
            event: A proton session error event.
        """
        self.handle_error(event, "session")

    def on_start(self, event):
        """
        Configure the AMQP container and start the connection when the proton reactor starts.

        Args:
            event: A proton event.
        """
        self.container = event.container
        self.container.selectable(self.injector)
        _log.debug("Starting the processor thread")
        self.processor = MessageProcessor(self.callback, self.injector)
        self._connect()

    def on_transport_error(self, event):
        """
        Handle an AMQP transport error.

        Args:
            event: A proton transport error event.
        """
        self.handle_error(event, "transport")

    def shutdown(self):
        """
        Clean up the remainder of the object to stop the reactor.

        Before calling this method, the processor thread must be stopped.

        Raises:
            self.fatal_exception: If a previous failure is the reason for this shutdown.
        """
        self.disconnect()

        if self.injector:
            self.injector.close()

        if self.container:
            self.container = None

        # TODO: Verify that this is raised after the connection is closed. If not, move this code
        # to on_connection_closed to see if this works.
        if self.fatal_exception:
            if isinstance(self.fatal_exception, tuple):
                # If it's a tuple, the context can remain
                six.reraise(*self.fatal_exception)
            else:
                raise self.fatal_exception

    @property
    def ssl_domain(self):
        """
        Get the :class:`proton.SSLDomain` used for client authentication.

        If ``self.tls`` does not have all the expected keys set, ``None`` will be returned.

        Returns:
            [type]: [description]
        """
        if not self.tls or all(not self.tls.get(key) for key in ("certfile", "keyfile", "ca_cert")):
            _log.warning("Skipping authentication because the TLS configuration was not provided")
            return

        domain = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)
        domain.set_credentials(self.tls["certfile"], self.tls["keyfile"], None)
        domain.set_trusted_ca_db(self.tls["ca_cert"])
        domain.set_peer_authentication(proton.SSLDomain.VERIFY_PEER)
        return domain
