# SPDX-License-Identifier: GPL-3.0-or-later
"""TODO."""
from __future__ import absolute_import, unicode_literals

# import enum
import logging

import proton
import proton.handlers
import proton.reactor

from messaging_poc.processor import MessageProcessor

log = logging.getLogger(__name__)


class Consumer(proton.handlers.MessagingHandler):
    """
    TODO
    """

    def __init__(self, callback):
        # TODO: What to do about prefetch?
        super(Consumer, self).__init__(prefetch=1, auto_accept=False, auto_settle=False)
        self.fatal_conditions.extend(
            ["amqp:resource-limit-exceeded", "amqp:decode-error", "amqp:invalid-field"]
        )
        self.connection = None
        self.container = None
        self.processor = MessageProcessor(callback, proton.reactor.EventInjector())
        self.receiver = None

    @property
    def ssl_domain(self):
        """TODO"""
        domain = proton.SSLDomain(proton.SSLDomain.MODE_CLIENT)
        domain.set_credentials("client.crt", "client.key", None)
        domain.set_trusted_ca_db("/etc/pki/tls/certs/ca-bundle.crt")
        domain.set_peer_authentication(proton.SSLDomain.VERIFY_PEER)
        return domain

    def on_callback_complete(self, event):
        """TODO"""
        disposition_state = event.subject
        print("Updating with %r" % disposition_state)
        if disposition_state == proton.Disposition.REJECTED:
            event.delivery.local.condition = proton.Condition(
                "amqp:precondition-failed", "TODO message"
            )
        event.delivery.update(disposition_state)
        # TODO: Added here for debugging
        # event.delivery.settle()
        # TODO: shutdown

    def on_connection_opened(self, event):
        """TODO"""
        print("Connected")
        self.connection = event.connection
        self.receiver = self.container.create_receiver(
            self.connection, "Consumer.client-mbs.mprahl-stage-test2.VirtualTopic.eng.>"
        )

    def on_message(self, event):
        """TODO"""
        print("Before callback")
        self.processor.process_message(event)

    def on_settled(self, event):
        print("Settling Locally")
        event.delivery.settle()

    def on_start(self, event):
        """TODO"""
        self.container = event.container
        # TODO: What should Container ID be used for?
        # self.container.container_id = 'mprahl-{0}-{1}'.format(str(uuid.uuid4()), 'client_name')
        self.container.selectable(self.processor.injector)
        urls = ["amqps://messaging-devops-broker02.web.stage.ext.phx2.redhat.com:5671"]
        # TODO: Make hearbeat configurable
        event.container.connect(urls=urls, ssl_domain=self.ssl_domain, heartbeat=20)
