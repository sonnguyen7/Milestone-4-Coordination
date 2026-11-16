import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from coordination import Ledger, LogicalClock, priority
from demo_coordination import run_demo


class CoordinationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.ledger = Ledger(self.folder / "test.db")
        self.epoch = self.ledger.elect("D3:incarnation")

    def op(self, op, **fields):
        return self.ledger.operate("D3:incarnation", self.epoch, op, dict(holder="D1", **fields))

    def test_lamport_update_and_concurrent_events(self):
        from concurrent.futures import ThreadPoolExecutor
        clock = LogicalClock("D1", self.folder)
        self.assertEqual(clock.event("LOCAL"), 1)
        self.assertEqual(clock.event("RECEIVE", remote=20), 21)
        with ThreadPoolExecutor(max_workers=8) as pool:
            values = list(pool.map(lambda _: clock.event("LOCAL"), range(100)))
        self.assertEqual(len(set(values)), 100)
        self.assertEqual(clock.value, 121)
        records = [json.loads(line) for line in clock.path.read_text().splitlines()]
        self.assertEqual([r["clock"] for r in records], sorted(r["clock"] for r in records))
        with self.assertRaises(ValueError):
            clock.event("BAD", remote=-1)

    def test_priority(self):
        self.assertGreater(priority("D10"), priority("D9"))

    def test_mutex_release_and_lease_expiry(self):
        with patch("coordination.time.time", return_value=100):
            first = self.op("ACQUIRE")
            self.assertEqual(self.op("ACQUIRE")["status"], "busy")
        with patch("coordination.time.time", return_value=106):
            second = self.op("ACQUIRE")
            self.assertEqual(second["status"], "granted")
            self.assertEqual(self.op("COMMIT", token=first["token"], resource="D1", request_id="r1", rider="r1")["status"], "invalid_grant")
            self.assertEqual(self.op("RELEASE", token=second["token"])["status"], "released")

    def test_booking_idempotency_and_conflicts(self):
        grant = self.op("ACQUIRE")
        fields = dict(token=grant["token"], resource="D1", request_id="r1", rider="rider1")
        self.assertEqual(self.op("COMMIT", **fields)["status"], "reserved")
        self.assertTrue(self.op("COMMIT", **fields)["duplicate"])
        self.assertEqual(self.op("COMMIT", **dict(fields, request_id="r2"))["status"], "conflict")
        self.assertEqual(self.op("COMMIT", **dict(fields, resource="D2"))["status"], "request_id_conflict")
        self.assertEqual(len(self.ledger.snapshot()["bookings"]), 1)

    def test_old_epoch_and_wrong_holder_rejected(self):
        grant = self.op("ACQUIRE")
        self.assertEqual(self.ledger.operate("D3:incarnation", self.epoch, "RELEASE", {"holder": "imposter", "token": grant["token"]})["status"], "invalid_grant")
        newer = self.ledger.elect("D2:new")
        self.assertGreater(newer, self.epoch)
        self.assertEqual(self.op("COMMIT", token=grant["token"], resource="D1", request_id="r", rider="r")["status"], "stale_leader")
        self.assertEqual(self.ledger.snapshot()["bookings"], [])

    def test_real_process_election_contention_crash_and_restart(self):
        summary = run_demo(self.folder / "cluster")
        self.assertEqual(summary["replacement_leader"], "D2")
        self.assertEqual(len(summary["final"]["ledger"]["bookings"]), 2)

    def test_existing_driver_http_assignment_uses_ledger(self):
        import threading
        import urllib.error
        import urllib.request
        from http.server import HTTPServer
        from driver_service import make_handler
        from peer_overlay import PeerNode
        from demo_coordination import wait_for
        with patch.dict("os.environ", {"COORDINATION_LOG_DIR": str(self.folder / "events")}):
            peer = PeerNode("D1", "127.0.0.1", 0, "127.0.0.1", 0, 0, 0,
                            lambda _: None, coordination_store=self.folder / "http.db")
        server = HTTPServer(("127.0.0.1", 0), make_handler("test-driver", "D1", peer))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        # Port zero is useful for tests; publish the actual listening address.
        peer.address = peer.server.server_address
        peer.identity["port"] = peer.address[1]
        try:
            wait_for(lambda: peer.coordination.leader == "D1")
            def assign(ride):
                req = urllib.request.Request(f"http://127.0.0.1:{server.server_port}/assign",
                    data=json.dumps({"ride_id": ride, "rider_id": "rider"}).encode(),
                    headers={"Content-Type": "application/json"})
                try:
                    with urllib.request.urlopen(req, timeout=15) as response:
                        return response.status
                except urllib.error.HTTPError as error:
                    error.close()
                    return error.code
            with patch("driver_service.log"):
                self.assertEqual(assign("ride-1"), 200)
                self.assertEqual(assign("ride-1"), 200)
                self.assertEqual(assign("ride-2"), 409)
            self.assertEqual(len(peer.coordination.ledger.snapshot()["bookings"]), 1)
            self.assertFalse(peer.current_resource()["available"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
            peer.stop()


if __name__ == "__main__":
    unittest.main(verbosity=2)
