class Kademlia:
    def __init__(self, routing_thread_in_queue, routing_thread_out_queue, refresh_interval_s=50):
        self.routing_thread_in_queue = routing_thread_in_queue
        self.routing_thread_out_queue = routing_thread_out_queue
        # Wait for the response; Ensure that client timeouts before the server
        self.timeout = refresh_interval_s*2

    def find_node(self, args):
        self.routing_thread_in_queue.put(f"find_node {args}")
        nearest_node = self.routing_thread_out_queue.get(True, timeout=self.timeout)
        return nearest_node

    def routing_table_show(self):
        self.routing_thread_in_queue.put(f"show")

    def ping(self):
        return "pong"

    def add(self, args):
        self.routing_thread_in_queue.put(f"add {args}")
        return "added"

    def show_node_view(self):
        self.routing_thread_in_queue.put(f"show_node_view")
