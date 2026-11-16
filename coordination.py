"""Milestone 4: Bully election and fenced, centralized mutual exclusion.

The SQLite ledger is a shared resource on ONE host, not a consensus protocol.
All peers must use the same COORDINATION_DB path. No third party packages.
"""
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager


def priority(node_id):
    """Natural ordering for D1..D10, deterministic ordering for other IDs."""
    if node_id.startswith("D") and node_id[1:].isdigit():
        return (1, int(node_id[1:]), node_id)
    return (0, 0, node_id)


class LogicalClock:
    def __init__(self, node_id, folder):
        self.value = 0
        self.node_id = node_id
        self.lock = threading.Lock()
        self.session = uuid.uuid4().hex
        Path(folder).mkdir(parents=True, exist_ok=True)
        self.path = Path(folder) / (node_id + "-events.jsonl")

    def event(self, kind, remote=0, **fields):
        if not isinstance(remote, int) or isinstance(remote, bool) or remote < 0:
            raise ValueError("logical timestamp must be a nonnegative integer")
        with self.lock:
            self.value = max(self.value, remote) + 1
            record = dict(fields, node=self.node_id, session=self.session,
                          clock=self.value, event=kind,
                          event_id=f"{self.node_id}:{self.session}:{self.value}")
            with self.path.open("a", encoding="utf-8") as out:
                out.write(json.dumps(record, sort_keys=True) + "\n")
            return self.value


class Ledger:
    """Atomic resource authority; epoch and lease checks fence obsolete grants.

    SQLite's transaction lock is an additional safety boundary: election timeouts
    alone cannot establish exclusive authority in an asynchronous network.
    """
    def __init__(self, path):
        self.path = str(path)
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS authority (
                  singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                  owner TEXT, epoch INTEGER NOT NULL, token TEXT,
                  holder TEXT, expires REAL);
                INSERT OR IGNORE INTO authority VALUES (1, NULL, 0, NULL, NULL, 0);
                CREATE TABLE IF NOT EXISTS bookings (
                  resource TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL,
                  rider TEXT NOT NULL, node TEXT NOT NULL, epoch INTEGER NOT NULL);
                CREATE TABLE IF NOT EXISTS history (
                  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                  event TEXT NOT NULL, details TEXT NOT NULL);
            ''')

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=5)
        db.row_factory = sqlite3.Row
        try:
            with db:
                yield db
        finally:
            db.close()

    @staticmethod
    def audit(db, event, **details):
        db.execute("INSERT INTO history(event, details) VALUES (?, ?)",
                   (event, json.dumps(details, sort_keys=True)))

    def elect(self, owner):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM authority").fetchone()
            if row["owner"] != owner:
                db.execute("UPDATE authority SET owner=?, epoch=epoch+1, token=NULL, holder=NULL, expires=0", (owner,))
                self.audit(db, "FENCE", owner=owner, epoch=row["epoch"] + 1)
            return db.execute("SELECT epoch FROM authority").fetchone()[0]

    def operate(self, owner, epoch, op, message):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM authority").fetchone()
            if row["owner"] != owner or row["epoch"] != epoch:
                return {"status": "stale_leader"}
            now = time.time()
            if op == "ACQUIRE":
                if row["token"] and row["expires"] > now:
                    return {"status": "busy"}
                token = uuid.uuid4().hex
                db.execute("UPDATE authority SET token=?, holder=?, expires=?", (token, message["holder"], now + 5))
                self.audit(db, "GRANT", holder=message["holder"], token=token, epoch=epoch)
                return {"status": "granted", "token": token, "epoch": epoch, "lease_seconds": 5}
            if (not row["token"] or row["token"] != message.get("token")
                    or row["holder"] != message.get("holder") or row["expires"] <= now):
                return {"status": "invalid_grant"}
            if op == "RELEASE":
                db.execute("UPDATE authority SET token=NULL, holder=NULL, expires=0")
                self.audit(db, "RELEASE", token=row["token"], epoch=epoch)
                return {"status": "released"}
            if op != "COMMIT":
                raise ValueError("unknown ledger operation")
            resource, request_id, rider = (message[k] for k in ("resource", "request_id", "rider"))
            if not all(isinstance(x, str) and 0 < len(x) <= 200 for x in (resource, request_id, rider)):
                raise ValueError("resource, request_id and rider must be strings of 1..200 characters")
            previous = db.execute("SELECT * FROM bookings WHERE request_id=?", (request_id,)).fetchone()
            if previous:
                same = previous["resource"] == resource and previous["rider"] == rider
                return {"status": "reserved" if same else "request_id_conflict", "duplicate": same}
            if db.execute("SELECT 1 FROM bookings WHERE resource=?", (resource,)).fetchone():
                self.audit(db, "CONFLICT", resource=resource, request_id=request_id, epoch=epoch)
                return {"status": "conflict", "resource": resource}
            db.execute("INSERT INTO bookings VALUES (?, ?, ?, ?, ?)",
                       (resource, request_id, rider, message["holder"], epoch))
            self.audit(db, "COMMIT", resource=resource, request_id=request_id,
                       holder=message["holder"], epoch=epoch, token=row["token"])
            return {"status": "reserved", "resource": resource, "epoch": epoch}

    def snapshot(self):
        with self.connect() as db:
            return {"authority": dict(db.execute("SELECT * FROM authority").fetchone()),
                    "bookings": [dict(r) for r in db.execute("SELECT * FROM bookings ORDER BY resource")],
                    "history": [dict(r) for r in db.execute("SELECT * FROM history ORDER BY sequence")]}


class Coordination:
    def __init__(self, node, store):
        self.node = node
        self.ledger = Ledger(store)
        self.owner = node.driver_id + ":" + node.identity["incarnation"]
        self.leader = None
        self.epoch = None
        self.lock = threading.RLock()
        self.electing = threading.Lock()
        self.stopped = threading.Event()
        self.thread = None

    def start(self):
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()

    def stop(self):
        self.stopped.set()
        if self.thread:
            self.thread.join(timeout=4)

    def monitor(self):
        while not self.stopped.wait(0.6):
            try:
                self.elect()
            except (OSError, ValueError, sqlite3.Error) as error:
                self.node.clock.event("ELECTION_RETRY", detail=str(error))

    def trigger(self):
        # A higher node answers OK immediately and conducts its own election.
        threading.Thread(target=self.elect, daemon=True).start()

    def elect(self):
        if self.stopped.is_set() or not self.electing.acquire(blocking=False):
            return
        try:
            higher = sorted((p for p in self.node._others()
                             if priority(p["id"]) > priority(self.node.driver_id)),
                            key=lambda p: priority(p["id"]), reverse=True)
            for peer in higher:
                try:
                    reply = self.node._call((peer["host"], peer["port"]),
                                            {"op": "C_ELECTION"}, timeout=0.4)
                    if reply["status"] == "ok":
                        with self.lock:
                            if self.leader == self.node.driver_id:
                                self.leader, self.epoch = None, None
                        return
                except (OSError, ValueError):
                    # Keep configured members: periodic probes allow recovery.
                    pass
            with self.lock:
                epoch = self.ledger.elect(self.owner)
                changed = self.leader != self.node.driver_id or self.epoch != epoch
                self.leader, self.epoch = self.node.driver_id, epoch
            if changed:
                self.node.clock.event("ELECTED", leader=self.leader, epoch=epoch)
                self.node.log(f"COORDINATOR leader={self.leader} epoch={epoch}")
            for peer in self.node._others():
                try:
                    self.node._call((peer["host"], peer["port"]),
                                    {"op": "C_COORDINATOR", "leader": self.node.driver_id,
                                     "epoch": epoch, "owner": self.owner}, timeout=0.4)
                except (OSError, ValueError):
                    pass
        finally:
            self.electing.release()

    def handle(self, message):
        op = message["op"]
        if op == "C_ELECTION":
            self.trigger()
            return {"status": "ok"}
        if op == "C_COORDINATOR":
            # Check the shared authority so delayed announcements cannot roll back state.
            authority = self.ledger.snapshot()["authority"]
            if authority["owner"] != message["owner"] or authority["epoch"] != message["epoch"]:
                return {"status": "stale_leader"}
            with self.lock:
                changed = self.leader != message["leader"] or self.epoch != message["epoch"]
                self.leader, self.epoch = message["leader"], message["epoch"]
            if changed:
                self.node.clock.event("LEADER", leader=self.leader, epoch=self.epoch)
            return {"status": "ok"}
        if op == "C_STATUS":
            with self.lock:
                return {"status": "ok", "node": self.node.driver_id,
                        "leader": self.leader, "epoch": self.epoch,
                        "ledger": self.ledger.snapshot()}
        if op == "C_RESERVE":
            return self.reserve(message["resource"], message["request_id"], message["rider"], message.get("hold", 0))
        if op in ("C_ACQUIRE", "C_COMMIT", "C_RELEASE"):
            with self.lock:
                if self.leader != self.node.driver_id or message.get("epoch") != self.epoch:
                    return {"status": "not_leader", "leader": self.leader}
                result = self.ledger.operate(self.owner, self.epoch, op[2:], message)
            self.node.clock.event(op[2:], outcome=result["status"], holder=message.get("holder"))
            return result
        raise ValueError("unknown coordination operation")

    def reserve(self, resource, request_id, rider, hold=0):
        hold = float(hold)
        if not 0 <= hold <= 2:
            raise ValueError("demo hold must be between 0 and 2 seconds")
        deadline = time.monotonic() + 12
        holder = self.owner + ":" + uuid.uuid4().hex
        while time.monotonic() < deadline and not self.stopped.is_set():
            with self.lock:
                leader, epoch = self.leader, self.epoch
            with self.node.lock:
                peer = self.node.members.get(leader)
                address = (peer["host"], peer["port"]) if peer else None
            if address is None:
                self.elect()
                self.stopped.wait(0.1)
                continue
            common = {"holder": holder, "epoch": epoch}
            try:
                grant = self.node._call(address, dict(common, op="C_ACQUIRE"), timeout=1)
                if grant["status"] != "granted":
                    self.stopped.wait(0.05)
                    continue
                common["token"] = grant["token"]
                self.node.clock.event("CS_ENTER", token=grant["token"], leader=leader, epoch=epoch)
                try:
                    self.stopped.wait(hold)
                    result = self.node._call(address, dict(common, op="C_COMMIT", resource=resource,
                                                           request_id=request_id, rider=rider), timeout=2)
                finally:
                    self.node.clock.event("CS_EXIT", token=grant["token"], leader=leader, epoch=epoch)
                    try:
                        self.node._call(address, dict(common, op="C_RELEASE"), timeout=1)
                    except (OSError, ValueError):
                        pass  # Grant expires; a new epoch also invalidates it.
                if result["status"] in ("reserved", "conflict", "request_id_conflict"):
                    return result
            except (OSError, ValueError):
                self.elect()
                self.stopped.wait(0.1)
        return {"status": "coordination_unavailable"}
