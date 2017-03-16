
import logging
import subprocess

import socket

from pathspider.base import PluggableSpider
from pathspider.base import CONN_OK
from pathspider.classic import SynchronizedSpider
from pathspider.helpers.tcp import connect_tcp
from pathspider.helpers.tcp import connect_http
from pathspider.observer import Observer
from pathspider.observer.base import BasicChain
from pathspider.observer.tcp import TCPChain
from pathspider.observer.tcp import TCP_SAE
from pathspider.observer.tcp import TCP_SAEC
from pathspider.observer.ecn import ECNChain

class ECN(SynchronizedSpider, PluggableSpider):

    def __init__(self, worker_count, libtrace_uri, args):
        super().__init__(worker_count=worker_count,
                         libtrace_uri=libtrace_uri,
                         args=args)
        self.conn_timeout = args.timeout

    def config_zero(self):
        """
        Disables ECN negotiation via sysctl.
        """

        logger = logging.getLogger('ecn')
        subprocess.check_call(['/sbin/sysctl', '-w', 'net.ipv4.tcp_ecn=2'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.debug("Configurator disabled ECN")

    def config_one(self):
        """
        Enables ECN negotiation via sysctl.
        """

        logger = logging.getLogger('ecn')
        subprocess.check_call(['/sbin/sysctl', '-w', 'net.ipv4.tcp_ecn=1'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.debug("Configurator enabled ECN")

    def connect(self, job, config):
        """
        Performs a TCP connection.
        """

        if self.args.connect == "tcp":
            rec = connect_tcp(self.source, job, self.conn_timeout)
        elif self.args.connect == "http":
            rec = connect_http(self.source, job, self.conn_timeout)
        else:
            raise RuntimeError("Unknown connection type requested!")

        return rec

    def create_observer(self):
        """
        Creates an observer with ECN-related chain functions.
        """

        logger = logging.getLogger('ecn')
        logger.info("Creating observer")
        return Observer(self.libtrace_uri,
                        chains=[BasicChain, TCPChain, ECNChain])

    def combine_flows(self, flows):
        conditions = []

        if flows[0]['spdr_state'] == CONN_OK and flows[1]['spdr_state'] == CONN_OK:
            conditions.append('ecn.connectivity.works')
        elif flows[0]['spdr_state'] == CONN_OK and not flows[1]['spdr_state'] == CONN_OK:
            conditions.append('ecn.connectivity.broken')
        elif not flows[0]['spdr_state'] == CONN_OK and flows[1]['spdr_state'] == CONN_OK:
            conditions.append('ecn.connectivity.transient')
        else:
            conditions.append('ecn.connectivity.offline')

        if flows[1]['observed'] and flows[1]['tcp_connected']:
            if flows[1]['tcp_synflags_rev'] & TCP_SAEC == TCP_SAE:
                conditions.append('ecn.negotiation.succeeded')
            elif flows[1]['tcp_synflags_rev'] & TCP_SAEC == TCP_SAEC:
                conditions.append('ecn.negotiation.reflected')
            else:
                conditions.append('ecn.negotiation.failed')

            conditions.append('ecn.ipmark.ect0.seen' if (flows[1]['ecn_ect0_syn_rev'] or flows[1]['ecn_ect0_data_rev'])
                              else 'ecn.ipmark.ect0.not_seen')
            conditions.append('ecn.ipmark.ect1.seen' if (flows[1]['ecn_ect1_syn_rev'] or flows[1]['ecn_ect1_data_rev'])
                              else 'ecn.ipmark.ect1.not_seen')
            conditions.append('ecn.ipmark.ce.seen' if (flows[1]['ecn_ce_syn_rev'] or flows[1]['ecn_ce_data_rev'])
                              else 'ecn.ipmark.ce.not_seen')

        return conditions

    @staticmethod
    def register_args(subparsers):
        parser = subparsers.add_parser('ecn', help="Explicit Congestion Notification")
        parser.add_argument("--timeout", default=5, type=int, help="The timeout to use for attempted connections in seconds (Default: 5)")
        parser.add_argument("--connect", type=str, choices=['http', 'tcp'], default='http',
                            metavar="[http|tcp]", help="Type of connection to perform (Default: http)")
        parser.set_defaults(spider=ECN)
