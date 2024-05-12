from binarytree import Node as BNode

import logging
from queue import Empty as QueueEmpty
import sys

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Node:
    def __init__(self, kid, value=None):
        self.kid = int(kid)
        self.bid = "{0:b}".format(self.kid)
        self.value = value

    def distance(self, other: 'Node'):
        # XOR distance between two nodes
        return self.kid ^ other.kid

    def __str__(self):
        return f'Node({self.bid})=>{self.value}'


class Universe:
    def __init__(self, depth: int):
        self.root = BNode("*")
        self.depth = depth

    def add(self, node: Node):
        node_repr = node.bid.zfill(self.depth)
        start = self.root
        for i, ch in enumerate(node_repr):
            if ch == "0":
                if start.right is None:
                    start.right = BNode(node_repr[:i+1])
                start = start.right
            else:
                if start.left is None:
                    start.left = BNode(node_repr[:i+1])
                start = start.left

    def find_node_by_value(self, node_val: str):
        start = self.root
        for i, ch in enumerate(node_val[:-1]):
            if ch == "0":
                if start.right is None:
                    return
                start = start.right
            else:
                if start.left is None:
                    return
                start = start.left
        if start.right and start.right.value == node_val:
            return start.right
        elif start.left and start.left.value == node_val:
            return start.left
        return None

    def remove(self, node_kid: str):
        bid = "{0:b}".format(int(node_kid))
        node_repr = bid.zfill(self.depth)
        new_leaves = [leaf for leaf in self.root.leaves if leaf.value != node_repr]
        self.root = BNode("*")
        for leaf in new_leaves:
            self.add(Node(int(leaf.value, 2)))

    def __str__(self):
        return str(self.root)


def node_view_thread_handler(depth, in_queue, timeout=5):
    universe = Universe(depth)
    while True:
        try:
            command = in_queue.get(True, timeout=timeout)
            command, *args = command.split(" ")
            if command == "add":
                logger.info(f"NodeView Adding {args}")
                universe.add(Node(*args))
            elif command == "show":
                logger.info(universe)
            elif command == "remove":
                logger.info(f"NodeView Removing {args}")
                universe.remove(*args)
            else:
                logger.info(f"Unknown command {command}")
        except QueueEmpty:
            logger.debug("Found no command in the queue")


if __name__ == "__main__":
    universe = Universe(4)
    universe.add(Node(0))
    universe.add(Node(15))
    universe.remove("0000")
    print(universe)