
import pycurl

import pathspider.base
from pathspider.base import PluggableSpider
from pathspider.base import CONN_OK
from pathspider.desync import DesynchronizedSpider
from pathspider.helpers.http import connect_http
from pathspider.helpers.http import connect_https
from pathspider.chains.basic import BasicChain
from pathspider.chains.tcp import TCPChain

class H2(DesynchronizedSpider, PluggableSpider):

    name = "h2"
    description = "HTTP/2"
    version = pathspider.base.__version__
    chains = [BasicChain, TCPChain]
    connect_supported = ["http", "https"]

    def conn_no_h2(self, job, config):  # pylint: disable=unused-argument
        if self.args.connect == "http":
            return connect_http(self.source, job, self.args.timeout)
        if self.args.connect == "https":
            return connect_http(self.source, job, self.args.timeout)
        else:
            raise RuntimeError("Unknown connection mode specified")

    def conn_h2(self, job, config): # pylint: disable=unused-argument
        curlopts = {pycurl.HTTP_VERSION: pycurl.CURL_HTTP_VERSION_2_0}
        curlinfos = {pycurl.INFO_HTTP_VERSION}
        if self.args.connect == "http":
            return connect_http(self.source, job, self.args.timeout, curlopts, curlinfos)
        if self.args.connect == "https":
            return connect_https(self.source, job, self.args.timeout, curlopts, curlinfos)
        else:
            raise RuntimeError("Unknown connection mode specified")

    connections = [conn_no_h2, conn_h2]

    def combine_flows(self, flows):
        conditions = []

        conditions.append(self.combine_connectivity(
                                             flows[0]['spdr_state'] == CONN_OK,
                                             flows[1]['spdr_state'] == CONN_OK))

        if flows[1]['spdr_state'] == CONN_OK:
            if flows[1]['http_info'][pycurl.INFO_HTTP_VERSION] == pycurl.CURL_HTTP_VERSION_2_0:
                conditions.append('h2.upgrade.success')
            else:
                conditions.append('h2.upgrade.failed')

        return conditions
