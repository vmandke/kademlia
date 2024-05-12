import json
import random
import time
from queue import Empty as QueueEmpty, Queue
from threading import Thread
import logging
import sys

from peer import Peer


MAX_TRIES = 3

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class KBucket:
    def __init__(self, k: int, node_view_thread_in_queue=None):
        self.k = k
        self.peers = []
        self.node_view_thread_in_queue = node_view_thread_in_queue

    def add(self, new_peer):
        new_peer_added = False
        if len(self.peers) < self.k:
            self.peers.append(new_peer)
            new_peer_added = True
        else:
            # Replace the oldest peer
            old_peer = self.peers.pop(0)
            peer_to_add = old_peer
            if not old_peer.ping():
                peer_to_add = new_peer
                new_peer_added = True
            self.peers.append(peer_to_add)
        if new_peer_added and self.node_view_thread_in_queue:
            logger.info(f"Adding {new_peer.kid} to node view")
            self.node_view_thread_in_queue.put(f"add {new_peer.kid}")

    def repr(self):
        return [str(peer) for peer in self.peers]

    def remove(self, peer_kid):
        peer_kid = int(peer_kid)
        possible_idx = [i for i, p in enumerate(self.peers) if p.kid == peer_kid]
        logger.info(f"KBucket::Removing {peer_kid} possible_idx {possible_idx}")
        if possible_idx:
            self.peers.pop(possible_idx[0])
            self.node_view_thread_in_queue.put(f"remove {peer_kid}")


class RoutingTable:
    def __init__(self, bid: str, depth: int, k: int = 1, node_view_thread_in_queue=None, owner_peer_config=None):
        self.depth = depth
        self.bid = bid.zfill(depth)
        self.k = k
        routing_table = {}
        # Create a prefix based routing table
        parsed_prefix = ""
        for ch in self.bid:
            to_find = "0" if ch == "1" else "1"
            routing_table[parsed_prefix + to_find] = KBucket(k, node_view_thread_in_queue)
            parsed_prefix += ch
        self.routing_table = routing_table
        self.owner_peer_config = owner_peer_config

    def remove(self, k_prefix, peer_kid):
        self.routing_table[k_prefix].remove(peer_kid)

    @staticmethod
    def cleanup(routing_table: 'RoutingTable'):
        logger.info("Routing Table :: Cleaning up")
        peers_to_remove = []
        for k_prefix, k_bucket in routing_table.routing_table.items():
            for peer in k_bucket.peers:
                if not peer.ping():
                    peers_to_remove.append((k_prefix, peer.kid))
        return peers_to_remove

    def find_nearest_node(self, bid):
        logger.info(f"Routing Table :: Finding nearest node to {bid}")
        node_id = int(bid.zfill(self.depth), 2)
        peers = [peer for k_bucket in self.routing_table.values() for peer in k_bucket.peers]
        peers.sort(key=lambda x: x.kid ^ node_id)
        nearest_peer = str(peers[0]) if len(peers) > 0 else None
        logger.info(f"Routing Table :: Nearest node to {bid} => {nearest_peer}")
        return nearest_peer

    def find_peer_in_prefix(self, bid, prefix, peers):
        # This should be on a separate thread
        logger.info(f"Routing Table :: Finding a peer in prefix {prefix} with random node {bid}")
        logger.info(f"Trying with {len(peers)} nearest peers")
        visited_kids = set()
        for peer in peers:
            peer_to_fetch_from = peer
            for _ in range(MAX_TRIES):
                if peer_to_fetch_from.kid in visited_kids:
                    continue
                peer_context, timeout = peer_to_fetch_from.find_node(bid)
                logger.info(f"Refresh::Peer::Found:: {peer_context} from {peer_to_fetch_from} fpr prefix {prefix} timeout {timeout}")
                if peer_context is None:
                    logger.info(f"Refresh::NoNode::{peer_to_fetch_from}")
                    break
                if timeout:
                    logger.info(f"Refresh::Timeout:: {peer_to_fetch_from}")
                    break
                peer_to_fetch_from = Peer(*peer_context.split(" "))
                peer_to_fetch_from.set_owner_peer_config(self.owner_peer_config)
                if peer_to_fetch_from.bid.startswith(prefix):
                    logger.info(f"Refresh::Found peer {peer_to_fetch_from} in prefix {prefix}")
                    self.routing_table[prefix].add(peer_to_fetch_from)
                    return peer_to_fetch_from
        return []

    @staticmethod
    def refresh_prefix(routing_table: 'RoutingTable', prefix: str):
        new_peer = None
        # This can be improved; however now lets loop over all peers
        random_node_bid = prefix + "".join([str(random.randint(0, 1)) for _ in range(routing_table.depth - len(prefix))])
        random_node_id = int(random_node_bid, 2)
        logger.info(f"Refresh:: Finding a peer with bid:: {random_node_bid}")
        peers = [peer for k_bucket in routing_table.routing_table.values() for peer in k_bucket.peers]
        peers.sort(key=lambda x: x.kid ^ random_node_id)
        if len(peers) == 0:
            logger.info("Refresh:: No peers found")
        else:
            logger.info(f"Refresh:: Found peers {str(peers)}")
            new_peer = routing_table.find_peer_in_prefix(random_node_bid, prefix, peers)
        return new_peer

    @staticmethod
    def get_empty_prefixes(routing_table: 'RoutingTable'):
        logger.info("Routing Table :: Getting empty prefixes")
        empty_prefixes = []
        for k_prefix, k_bucket in routing_table.routing_table.items():
            if len(k_bucket.peers) == 0:
                empty_prefixes.append(k_prefix)
        return empty_prefixes

    @staticmethod
    def refresh(routing_thread_in_queue, refresh_queue, refresh_interval_s):
        while True:
            routing_thread_in_queue.put(f"refresh_get_config")
            table_config = refresh_queue.get(True, timeout=10)
            routing_table = RoutingTable.rebuild_from_str(table_config)
            removables = RoutingTable.cleanup(routing_table)
            logger.info(f"Routing Table :: Cleanup :: {removables}")
            for k_bucket, peer_kid in removables:
                routing_thread_in_queue.put(f"refresh_remove {k_bucket} {peer_kid}")
            routing_thread_in_queue.put(f"refresh_get_config")
            table_config = refresh_queue.get(True, timeout=10)
            routing_table = RoutingTable.rebuild_from_str(table_config)
            empty_prefixes = RoutingTable.get_empty_prefixes(routing_table)
            for prefix in empty_prefixes:
                new_peer = RoutingTable.refresh_prefix(routing_table, prefix)
                if new_peer:
                    routing_thread_in_queue.put(f"add {new_peer}")
            routing_thread_in_queue.put(f"refresh_get_config")
            time.sleep(10)

    def add(self, peer):
        peer_prefix = peer.bid.zfill(self.depth)
        for k_prefix, k_bucket in self.routing_table.items():
            if peer_prefix.startswith(k_prefix):
                k_bucket.add(peer)
                peer.set_owner_peer_config(self.owner_peer_config)
                break

    def __str__(self):
        config = {
            "bid": self.bid,
            "depth": self.depth,
            "k": self.k,
            "routing_table": {k_prefix: k_bucket.repr() for k_prefix, k_bucket in self.routing_table.items()},
            "owner_peer_config": self.owner_peer_config if self.owner_peer_config else None
        }
        logger.info(f"Routing Table :: {config}")
        return json.dumps(config)

    @staticmethod
    def rebuild_from_str(config):
        logger.info(f"Rebuilding routing table from {config}")
        config = json.loads(config)
        routing_table = RoutingTable(
            config["bid"], config["depth"], config["k"], owner_peer_config=config["owner_peer_config"]
        )
        for k_prefix, k_bucket in config["routing_table"].items():
            for _peer in k_bucket:
                peer = Peer(*_peer.split(" "))
                routing_table.add(peer)
        return routing_table


def routing_thread_handler(
        k, bid, depth, routing_thread_in_queue, routing_thread_out_queue, node_view_thread_in_queue,
        bootstrap_config=None, refresh_interval_s=5, owner_peer_config=None
):
    routing_table = RoutingTable(bid, depth, k, node_view_thread_in_queue, owner_peer_config)
    node_view_thread_in_queue.put(f"add {int(bid, 2)} peer")
    refresh_queue = Queue()
    if bootstrap_config:
        joining_peer = Peer(bootstrap_config["bid"], bootstrap_config["ip"], bootstrap_config["port"])
        routing_table.add(joining_peer)
        node_view_thread_in_queue.put(f"add {int(bootstrap_config['bid'], 2)} peer")

    logger.info(f"Starting refresh thread with interval {refresh_interval_s} seconds")
    refresh_thread = Thread(
        target=RoutingTable.refresh,
        args=(routing_thread_in_queue, refresh_queue, refresh_interval_s)
    )
    refresh_thread.start()

    while True:
        try:
            command = routing_thread_in_queue.get(True, timeout=refresh_interval_s)
            if "caller" in command:
                command, caller = command.split("caller")
                logger.info(f"Caller::: {caller} for command {command}")
                # Add the caller to the routing table
                routing_table.add(Peer(*caller.strip().split(" ")))
            command, *args = command.split(" ")
            logger.info(f"Handling Command {command} with args {args}")
            if command == "add":
                routing_table.add(Peer(*args))
            elif command == "refresh_remove":
                logger.info(f"RefreshRemoving {args[1]} from {args[0]}")
                routing_table.remove(*args)
            elif command == "refresh_get_config":
                refresh_queue.put(str(routing_table))
            elif command == "find_node":
                nearest_node = routing_table.find_nearest_node(args[0])
                logger.info(f"Nearest node to {args[0]} is {nearest_node}")
                routing_thread_out_queue.put(nearest_node)
            elif command == "show":
                logger.info(routing_table)
            elif command == "show_node_view":
                node_view_thread_in_queue.put(f"show")
            else:
                logger.info(f"Unknown command {command}")
        except QueueEmpty:
            logger.debug("Found no command in the queue")
