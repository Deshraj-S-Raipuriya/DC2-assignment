# dme.py
# Lamport Distributed Mutual Exclusion middleware
# Each client using this middleware must supply:
# - my_id: unique identifier string
# - my_port: port where this node listens for DME messages
# - peers: list of ("host", port, id) for other clients
#
# Provides:
# - request_critical_section() -> blocks until allowed
# - release_critical_section()

import socket
import threading
import json
import time
from queue import PriorityQueue

class LamportDME:
    def __init__(self, my_id, my_port, peers):
        self.my_id = my_id
        self.my_port = my_port
        self.peers = peers  # list of dicts: {"host":..., "port":..., "id":...}
        self.clock = 0
        self.lock = threading.Lock()

        # Replies management
        self.replies_needed = set()
        self.replies_event = threading.Event()

        # Request queue: items are (timestamp, id)
        self.request_queue = PriorityQueue()

        # Start listener for incoming DME messages
        self.running = True
        t = threading.Thread(target=self._listener, daemon=True)
        t.start()

    def _increment_clock(self, other_ts=None):
        with self.lock:
            if other_ts is None:
                self.clock += 1
            else:
                self.clock = max(self.clock, other_ts) + 1
            return self.clock

    def _listener(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind(('0.0.0.0', self.my_port))
        s.listen(5)
        while self.running:
            try:
                conn, addr = s.accept()
                data = conn.recv(65536).decode()
                if not data:
                    conn.close()
                    continue
                msg = json.loads(data)
                typ = msg.get("type")
                if typ == "REQUEST":
                    ts = msg["timestamp"]
                    nid = msg["node_id"]
                    # update clock
                    self._increment_clock(ts)
                    # enqueue request
                    self.request_queue.put((ts, nid))
                    # send REPLY
                    reply = {"type":"REPLY", "timestamp": self._increment_clock(), "node_id": self.my_id}
                    conn.sendall(json.dumps(reply).encode())
                elif typ == "RELEASE":
                    ts = msg["timestamp"]
                    nid = msg["node_id"]
                    # update clock
                    self._increment_clock(ts)
                    # remove from queue: reconstruct queue without the released request
                    self._remove_request(nid, ts)
                    conn.sendall(json.dumps({"type":"ACK"}).encode())
                elif typ == "REPLY":
                    ts = msg["timestamp"]
                    nid = msg["node_id"]
                    self._increment_clock(ts)
                    # mark reply received
                    with self.lock:
                        if nid in self.replies_needed:
                            self.replies_needed.remove(nid)
                            if not self.replies_needed:
                                self.replies_event.set()
                    conn.sendall(json.dumps({"type":"ACK"}).encode())
                else:
                    conn.sendall(json.dumps({"status":"ERR", "message":"unknown type"}).encode())
                conn.close()
            except Exception as e:
                # continue listening
                continue
        s.close()

    def _remove_request(self, nid, ts):
        # PriorityQueue doesn't allow direct removal — rebuild
        items = []
        while not self.request_queue.empty():
            items.append(self.request_queue.get())
        # remove matching
        items = [it for it in items if it[1] != nid]
        for it in items:
            self.request_queue.put(it)
        print(f"[DME] Queue after removing {nid}: {list(self.request_queue.queue)}")

    def _send_to_peer(self, peer, msg, expect_response=False, timeout=5):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect((peer["host"], peer["port"]))
            s.sendall(json.dumps(msg).encode())
            if expect_response:
                data = s.recv(65536).decode()
                if data:
                    return json.loads(data)
            s.close()
        except Exception as e:
            # network error or peer down; if peer is down then we treat as non-responsive
            print(f"[DME] Failed to contact peer {peer['id']}: {e}")
            with self.lock:
                self.replies_needed.discard(peer["id"])

            return None
        return None

    def request_critical_section(self):
        # increment local clock and create request timestamp
        ts = self._increment_clock()
        # enqueue own request
        self.request_queue.put((ts, self.my_id))
        # prepare set of peers required replies
        self.replies_needed = set(p["id"] for p in self.peers)
        # send REQUEST to all peers
        req_msg = {"type":"REQUEST", "timestamp": ts, "node_id": self.my_id}
        for p in self.peers:
            # best-effort send; REPLY may come later
            self._send_to_peer(p, req_msg, expect_response=True)
        # Wait until all replies received and own request is at the head of queue
        # Wait for replies_event
        # But also check queue head
        # Use a loop with timeout to avoid deadlock if a peer is down:
        start = time.time()
        while True:
            # first: wait for replies (but don't block forever)
            if not self.replies_event.is_set():
                # short sleep wait
                time.sleep(0.1)
            # check if we have replies or peers are unreachable
            with self.lock:
                have_all_replies = (not self.replies_needed)
            # check queue head
            head_ok = False
            if not self.request_queue.empty():
                head_ts, head_id = self.request_queue.queue[0]
                if head_id == self.my_id and head_ts == ts:
                    head_ok = True
            if have_all_replies and head_ok:
                # Enter critical section
                self.replies_event.clear()
                return ts  # return timestamp used for this critical section
            # safety: if waiting too long (e.g., peer down), assume peers that didn't reply are down
            if time.time() - start > 10:
                # We will proceed if we are first in queue among alive nodes. For demo simplicity,
                # if some peers didn't reply we treat them as down after timeout.
                # If someone else is ahead in queue but doesn't reply, risk exists but for lab this is okay.
                with self.lock:
                    if not self.request_queue.empty():
                        head_ts, head_id = self.request_queue.queue[0]
                        if head_id == self.my_id and head_ts == ts:
                            self.replies_event.clear()
                            return ts
                # otherwise keep waiting a bit more
                start = time.time()
                print(f"[DME] Waiting for replies from: {self.replies_needed}")
                print(f"[DME] Queue state: {list(self.request_queue.queue)}")


    def release_critical_section(self, ts):
        # remove own request from queue
        self._remove_request(self.my_id, ts)
        # increment clock and broadcast RELEASE
        rts = self._increment_clock()
        rel_msg = {"type":"RELEASE", "timestamp": rts, "node_id": self.my_id}
        for p in self.peers:
            self._send_to_peer(p, rel_msg, expect_response=True)

    def stop(self):
        self.running = False
