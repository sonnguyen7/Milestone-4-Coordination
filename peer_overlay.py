"""Small successor-routed peer overlay for discovering live driver services.
Peers know membership addresses, but only the owner knows its current driver
availability. A lookup visits peers over TCP, rather than querying the old
matching-service registration table. JSON messages are newline framed.
"""
import json
import socket
import socketserver
import threading
import uuid
import os
from pathlib import Path
from coordination import LogicalClock, Coordination
MAX_MESSAGE = 65536
_client_lock = threading.Lock()
_client_clock = 0

def endpoint(value):
    host, separator, port = value.rpartition(":")
    if not separator or not host or not port.isdigit():
        raise ValueError("expected host:port")
    return host, int(port)

def call(address, message, timeout=1.5):
    global _client_clock
    # Legacy clients gain logical timestamps without changing their CLI.
    with _client_lock:
        _client_clock += 1
        message = dict(message)
        message.setdefault("clock", _client_clock)
        message.setdefault("sender", "external-client")
        message.setdefault("message_id", uuid.uuid4().hex)
    with socket.create_connection(address, timeout=timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall((json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8"))
        raw = sock.makefile("rb").readline(MAX_MESSAGE + 1)
    if not raw or len(raw) > MAX_MESSAGE:
        raise OSError("missing or oversized peer reply")
    reply = json.loads(raw)
    with _client_lock:
        _client_clock = max(_client_clock, reply.get("clock", 0)) + 1
    if reply.get("status") == "error":
        raise ValueError(reply.get("detail", "peer error"))
    return reply

def query(seeds, kind="scan", target_id=None, trace=True):
    """Try known entry points; routing and discovery happen inside the overlay."""
    message = {"op": "LOOKUP", "kind": kind, "target_id": target_id,
               "query_id": uuid.uuid4().hex, "path": [], "ttl": 32, "trace": trace}
    last_error = None
    for seed in seeds:
        try:
            return call(seed, message, timeout=4)
        except (OSError, ValueError) as error:
            last_error = error
    raise OSError(f"no reachable peer entry point: {last_error}")

class _Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

class _Handler(socketserver.StreamRequestHandler):
    def handle(self):
        try:
            raw = self.rfile.readline(MAX_MESSAGE + 1)
            if not raw or len(raw) > MAX_MESSAGE:
                raise ValueError("empty or oversized message")
            response = self.server.node.handle_message(json.loads(raw))
        except (ValueError, KeyError, TypeError) as error:
            response = {"status": "error", "detail": str(error),
                        "clock": self.server.node.clock.event("SEND_ERROR"),
                        "sender": self.server.node.driver_id}
        self.wfile.write((json.dumps(response, separators=(",", ":")) + "\n").encode("utf-8"))

class PeerNode:
    def __init__(self, driver_id, peer_host, peer_port, http_host, http_port,
                 lat, lon, log, coordination_store=None):
        self.driver_id = driver_id
        self.address = (peer_host, peer_port)
        self.resource = {"driver_id": driver_id, "host": http_host,
                         "port": http_port, "lat": lat, "lon": lon,
                         "available": True}
        self.identity = {"id": driver_id, "host": peer_host, "port": peer_port,
                         "incarnation": uuid.uuid4().hex}
        self.members = {driver_id: self.identity}
        self.lock = threading.RLock()
        self.log = log
        folder = os.environ.get("COORDINATION_LOG_DIR", str(Path(__file__).parent / "logs" / "coordination"))
        self.clock = LogicalClock(driver_id, folder)
        store = coordination_store or os.environ.get("COORDINATION_DB", str(Path(__file__).parent / "data" / "coordination.db"))
        self.coordination = Coordination(self, store)
        self.server = _Server(self.address, _Handler)
        self.server.node = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.log(f"START {driver_id} peer={peer_host}:{peer_port}")
        self.coordination.start()

    def _call(self, address, message, timeout=1.5):
        message_id = uuid.uuid4().hex
        stamp = self.clock.event("SEND", op=message["op"], message_id=message_id,
                                 target=list(address))
        outgoing = dict(message, clock=stamp, sender=self.driver_id, message_id=message_id)
        reply = call(address, outgoing, timeout)
        self.clock.event("RECEIVE_REPLY", remote=reply.get("clock", 0),
                         message_id=message_id, remote_clock=reply.get("clock", 0))
        return reply

    def join(self, bootstrap):
        reply = self._call(bootstrap, {"op": "JOIN", "peer": self.identity})
        self._merge(reply["members"])
        # Announce to every known member. Data lookups still use overlay hops.
        for peer in self._others():
            try:
                self._call((peer["host"], peer["port"]),
                     {"op": "ANNOUNCE", "peer": self.identity})
            except OSError:
                self._remove(peer["id"], peer["incarnation"])
        self.log(f"JOIN membership={self.member_ids()} neighbors={self.neighbors()}")

    def stop(self, graceful=True):
        self.coordination.stop()
        if graceful:
            for peer in self._others():
                try:
                    self._call((peer["host"], peer["port"]),
                         {"op": "LEAVE", "peer": self.identity}, timeout=0.5)
                except OSError:
                    pass
            self.log(f"LEAVE {self.driver_id}")
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def reserve(self, request_id=None, rider="legacy-rider"):
        result = self.coordination.reserve(self.driver_id, request_id or uuid.uuid4().hex, rider)
        if result["status"] != "reserved":
            return False
        with self.lock:
            self.resource["available"] = False
            return True

    def current_resource(self):
        with self.lock:
            own = dict(self.resource)
        booked = {b["resource"] for b in self.coordination.ledger.snapshot()["bookings"]}
        own["available"] = own["available"] and self.driver_id not in booked
        return own

    def update_location(self, lat, lon):
        with self.lock:
            self.resource["lat"] = float(lat)
            self.resource["lon"] = float(lon)

    def member_ids(self):
        with self.lock:
            return sorted(self.members)

    def neighbors(self):
        ids = self.member_ids()
        if len(ids) < 2:
            return []
        pos = ids.index(self.driver_id)
        return list(dict.fromkeys((ids[(pos - 1) % len(ids)],
                                   ids[(pos + 1) % len(ids)])))

    def _others(self):
        with self.lock:
            return [dict(peer) for key, peer in self.members.items()
                    if key != self.driver_id]

    def _merge(self, peers):
        with self.lock:
            for peer in peers:
                if peer["id"] != self.driver_id:
                    self.members[peer["id"]] = peer

    def _remove(self, driver_id, incarnation):
        with self.lock:
            old = self.members.get(driver_id)
            if old and old.get("incarnation") == incarnation and driver_id != self.driver_id:
                del self.members[driver_id]
                return True
        return False

    def _failed(self, peer):
        if self._remove(peer["id"], peer["incarnation"]):
            self.log(f"FAIL {peer['id']} unreachable; neighbors={self.neighbors()}")
            for other in self._others():
                try:
                    self._call((other["host"], other["port"]),
                         {"op": "REMOVE", "peer": peer}, timeout=0.5)
                except OSError:
                    pass

    def _next(self, path):
        with self.lock:
            ids = sorted(self.members)
            pos = ids.index(self.driver_id)
            for offset in range(1, len(ids)):
                peer_id = ids[(pos + offset) % len(ids)]
                if peer_id not in path:
                    return dict(self.members[peer_id])
        return None

    def handle_message(self, message):
        stamp = message.get("clock", 0)
        self.clock.event("RECEIVE", remote=stamp, remote_clock=stamp,
                         sender=message.get("sender", "external-client"),
                         message_id=message.get("message_id"), op=message["op"])
        response = self._dispatch(message)
        response["clock"] = self.clock.event("SEND_REPLY", op=message["op"],
                                             message_id=message.get("message_id"))
        response["sender"] = self.driver_id
        return response

    def _dispatch(self, message):
        op = message["op"]
        if op.startswith("C_"):
            return self.coordination.handle(message)
        if op == "JOIN":
            peer = message["peer"]
            self._merge([peer])
            self.log(f"JOIN {peer['id']} neighbors={self.neighbors()}")
            with self.lock:
                return {"status": "ok", "members": list(self.members.values())}
        if op == "ANNOUNCE":
            self._merge([message["peer"]])
            self.log(f"ANNOUNCE {message['peer']['id']} neighbors={self.neighbors()}")
            return {"status": "ok"}
        if op in ("LEAVE", "REMOVE"):
            peer = message["peer"]
            if self._remove(peer["id"], peer["incarnation"]):
                self.log(f"{op} {peer['id']} neighbors={self.neighbors()}")
            return {"status": "ok"}
        if op == "STATUS":
            with self.lock:
                return {"status": "ok", "peer": self.identity,
                        "members": sorted(self.members),
                        "neighbors": self.neighbors(),
                        "resource": self.current_resource()}
        if op == "UPDATE_LOCATION":
            self.update_location(message["lat"], message["lon"])
            self.log(f"LOCATION {self.driver_id} ({message['lat']}, {message['lon']})")
            return {"status": "updated"}
        if op == "LOOKUP":
            return self._lookup(message)
        raise ValueError(f"unknown operation: {op}")

    def _lookup(self, message):
        kind = message["kind"]
        if kind not in ("scan", "find"):
            raise ValueError("lookup kind must be scan or find")
        path = list(message.get("path", []))
        if self.driver_id in path:
            raise ValueError("lookup loop detected")
        path.append(self.driver_id)
        trace = bool(message.get("trace", False))
        query_id = message.get("query_id", "unknown")
        if trace:
            self.log(f"LOOKUP {query_id} kind={kind} target={message.get('target_id')} path={'->'.join(path)}")
        own = self.current_resource()
        resources = ([own] if kind == "scan" or message.get("target_id") == self.driver_id
                     else [])
        if kind == "find" and resources:
            if trace:
                self.log(f"FOUND {query_id} resource={self.driver_id} path={'->'.join(path)}")
            return {"status": "found", "path": path, "resources": resources,
                    "query_id": query_id}
        remaining = int(message.get("ttl", 0))
        while remaining > 0:
            peer = self._next(path)
            if peer is None:
                break
            if trace:
                self.log(f"FORWARD {query_id} {self.driver_id}->{peer['id']}")
            outgoing = dict(message, path=path, ttl=remaining - 1)
            try:
                reply = self._call((peer["host"], peer["port"]), outgoing, timeout=3)
                return {"status": reply["status"], "path": reply["path"],
                        "resources": resources + reply["resources"],
                        "query_id": query_id}
            except OSError:
                self._failed(peer)
                remaining -= 1
        return {"status": "complete" if kind == "scan" else "missing",
                "path": path, "resources": resources, "query_id": query_id}
