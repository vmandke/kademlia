import zerorpc
from datetime import datetime

import logging
import sys


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Peer:
    def __init__(self, bid: str, ip: str, port: str):
        logger.info(f"Creating peer {bid} {ip} {port}")
        self.bid = bid
        self.ip = ip
        self.port = int(port)
        self.kid = int(bid, 2)
        # FIXME(vin) Heartbeat doesnt seem to work
        self.owner_peer_config = None
        # A worker can only add a node that's seen
        self.last_seen = datetime.utcnow()

    def __str__(self):
        return f"{self.bid} {self.ip} {self.port}"

    def set_owner_peer_config(self, owner_peer_config):
        self.owner_peer_config = owner_peer_config

    def find_node(self, bid: str):
        try:
            client = zerorpc.Client(f"tcp://{self.ip}:{self.port}", timeout=10, heartbeat=None)
            logger.info(f"Find Node {str(self)} {bid} with owner {self.owner_peer_config}")
            peer_context = client.find_node(f"{bid} caller {self.owner_peer_config}")
            client.close()
            return (peer_context, False) if peer_context != self.owner_peer_config else (None, False)
        except zerorpc.exceptions.TimeoutExpired:
            return None, True

    def ping(self):
        try:
            client = zerorpc.Client(f"tcp://{self.ip}:{self.port}", timeout=2, heartbeat=None)
            logger.info(f"Ping {str(self)}")
            retvalue = client.ping()
            logger.info(f"Ping {str(self)}=>{retvalue}")
            client.close()
            self.last_seen = datetime.utcnow()
            return True
        except zerorpc.exceptions.TimeoutExpired:
            return False


