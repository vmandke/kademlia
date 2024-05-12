import click
import zerorpc

from threading import Thread
from queue import Queue
import logging
import sys

from routing import routing_thread_handler
from node import node_view_thread_handler
from worker import Kademlia

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REFRESH_INTERVAL = 50


@click.command()
@click.option("--bid", default="0000", help="Bid of the node")
@click.option("--depth", default=4, help="Depth of the routing table")
@click.option("--k", default=1, help="Number of peers in each bucket")
@click.option("--ip", default="0.0.0.0", help="IP of the node")
@click.option("--port", default="4242", help="Port of the node")
@click.option("--bootstrap-bid", default=None, help="Bid of the bootstrap node")
@click.option("--bootstrap-ip", default=None, help="IP of the bootstrap node")
@click.option("--bootstrap-port", default=None, help="Port of the bootstrap node")
def runner(k, bid, depth, ip, port, bootstrap_bid, bootstrap_ip, bootstrap_port):
    logger.info(f"Starting Kademlia node with bid {bid} and depth {depth} and k {k}")
    routing_thread_in_queue = Queue()
    routing_thread_out_queue = Queue()
    node_view_thread_in_queue = Queue()

    bootstrap_config = None
    if bootstrap_bid and bootstrap_ip and bootstrap_port:
        bootstrap_config = {
            "bid": bootstrap_bid,
            "ip": bootstrap_ip,
            "port": bootstrap_port
        }
        logger.info(f"Bootstrapping with {bootstrap_config}")

    owner_peer_config = f"{bid} {ip} {port}"

    routing_thread = Thread(
        target=routing_thread_handler,
        args=(
            k, bid, depth,
            routing_thread_in_queue, routing_thread_out_queue, node_view_thread_in_queue,
            bootstrap_config, REFRESH_INTERVAL, owner_peer_config
        )
    )
    routing_thread.start()
    node_view_thread = Thread(
        target=node_view_thread_handler,
        args=(depth, node_view_thread_in_queue)
    )
    node_view_thread.start()
    srv = zerorpc.Server(Kademlia(routing_thread_in_queue, routing_thread_out_queue))
    srv.bind(f"tcp://{ip}:{port}")
    logger.info(f"Kademlia node started at {ip}:{port}")
    srv.run()


if __name__ == "__main__":
    runner()
