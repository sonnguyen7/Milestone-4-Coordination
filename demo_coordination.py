"""Repeatable real-process demo. Uses temporary ports and a fresh evidence folder."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
import uuid

from coordination import Ledger
from ordered_logs import ordered, verify
from peer_overlay import call, query


def wait_for(predicate, timeout=15):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError, KeyError) as error:
            last = error
        time.sleep(0.1)
    raise AssertionError(f"condition did not converge within {timeout}s: {last}")


class Cluster:
    def __init__(self, folder):
        self.folder = Path(folder).resolve()
        self.folder.mkdir(parents=True, exist_ok=True)
        self.db = self.folder / "coordination.db"
        self.processes = {}
        self.outputs = []
        self.addresses = {}
        # Hold sockets until all addresses have been allocated.
        sockets = []
        try:
            for node in ("D1", "D2", "D3"):
                sock = socket.socket()
                sock.bind(("127.0.0.1", 0))
                sockets.append(sock)
                self.addresses[node] = sock.getsockname()
        finally:
            for sock in sockets:
                sock.close()

    def start(self, node):
        env = dict(os.environ, COORDINATION_DB=str(self.db),
                   COORDINATION_LOG_DIR=str(self.folder / "events"))
        args = [sys.executable, "-u", str(Path(__file__).with_name("coordination_node.py")),
                node, str(self.addresses[node][1])]
        live = [n for n, p in self.processes.items() if p.poll() is None and n != node]
        if live:
            address = self.addresses[live[0]]
            args += ["--seed", f"{address[0]}:{address[1]}"]
        out = (self.folder / f"{node}-console.log").open("a", encoding="utf-8")
        self.outputs.append(out)
        self.processes[node] = subprocess.Popen(args, env=env, stdout=out, stderr=subprocess.STDOUT)
        wait_for(lambda: self.status(node))
        return self.processes[node]

    def rpc(self, node, message, timeout=15):
        return call(self.addresses[node], message, timeout=timeout)

    def status(self, node):
        return self.rpc(node, {"op": "C_STATUS"}, timeout=1)

    def converged(self, leader, nodes):
        statuses = [self.status(n) for n in nodes]
        return all(s["leader"] == leader and s["epoch"] == statuses[0]["epoch"] for s in statuses)

    def reserve(self, node, resource, request_id, hold=0):
        return self.rpc(node, {"op": "C_RESERVE", "resource": resource,
                               "request_id": request_id, "rider": request_id, "hold": hold})

    def kill(self, node):
        proc = self.processes[node]
        proc.kill()
        proc.wait(timeout=5)

    def close(self):
        for proc in self.processes.values():
            if proc.poll() is None:
                proc.kill()
        for proc in self.processes.values():
            proc.wait(timeout=5)
        for out in self.outputs:
            out.close()


def run_demo(folder):
    cluster = Cluster(folder)
    try:
        for node in cluster.addresses:
            cluster.start(node)
        wait_for(lambda: cluster.converged("D3", ["D1", "D2", "D3"]))
        before = cluster.status("D1")
        print(f"ELECTION: D3 is leader, epoch {before['epoch']}", flush=True)
        discovery = query([cluster.addresses["D1"]], trace=False)
        assert discovery["path"] == ["D1", "D2", "D3"], discovery
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(cluster.reserve, n, "D1", f"competing-{n}", 0.4) for n in ("D1", "D2")]
            results = [f.result() for f in futures]
        assert sorted(r["status"] for r in results) == ["conflict", "reserved"], results
        print("MUTEX: simultaneous requests -> one reserved, one conflict", flush=True)
        # Crash a real leader process with an outstanding grant.
        old = cluster.status("D3")
        grant = cluster.rpc("D3", {"op": "C_ACQUIRE", "epoch": old["epoch"], "holder": "failure-demo"})
        assert grant["status"] == "granted", grant
        cluster.kill("D3")
        wait_for(lambda: cluster.converged("D2", ["D1", "D2"]))
        after = cluster.status("D1")
        assert after["epoch"] > before["epoch"]
        stale = Ledger(cluster.db).operate(old["ledger"]["authority"]["owner"], old["epoch"], "COMMIT",
                   {"holder": "failure-demo", "token": grant["token"], "resource": "D2",
                    "request_id": "stale-request", "rider": "stale-rider"})
        assert stale["status"] == "stale_leader", stale
        assert cluster.reserve("D1", "D2", "after-failure")["status"] == "reserved"
        print(f"FAILOVER: killed D3 while holding grant; D2 elected, epoch {after['epoch']}; stale grant rejected", flush=True)
        cluster.start("D3")
        wait_for(lambda: cluster.converged("D3", ["D1", "D2", "D3"]))
        assert cluster.reserve("D2", "D1", "still-conflicts")["status"] == "conflict"
        print("RECOVERY: restarted D3 wins Bully election; earlier booking remains protected", flush=True)
        final = cluster.status("D1")
    finally:
        cluster.close()
    events = ordered(cluster.folder / "events")
    checks = verify(events)
    # Resource commits are serialized inside each live grant, including across fencing.
    active = None
    for row in final["ledger"]["history"]:
        details = json.loads(row["details"])
        if row["event"] == "FENCE":
            active = None
        elif row["event"] == "GRANT":
            assert active is None, "overlapping live grants"
            active = details["token"]
        elif row["event"] == "COMMIT":
            assert active == details["token"], "commit outside grant"
        elif row["event"] == "RELEASE":
            assert active == details["token"]
            active = None
    summary = {"checks": checks, "competition": results, "initial_leader": before["leader"],
               "replacement_leader": after["leader"], "stale_grant": stale,
               "final": final, "baseline_overlay_path": discovery["path"]}
    (cluster.folder / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (cluster.folder / "ordered-events.jsonl").write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
    print(f"PASS: {checks}; evidence: {cluster.folder}", flush=True)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()
    # Never reuse or delete an existing ledger as part of a demo.
    folder = Path(args.output or "demo-evidence") / ("run-" + uuid.uuid4().hex[:10])
    run_demo(folder)


if __name__ == "__main__":
    main()
