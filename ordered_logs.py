"""Merge peer logs in deterministic (Lamport clock, node, session) order.

This is an offline ordering, not a live total-order broadcast protocol.
"""
import argparse
import json
from pathlib import Path


def ordered(folder):
    events = []
    for path in Path(folder).glob("*-events.jsonl"):
        events.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line)
    return sorted(events, key=lambda e: (e["clock"], e["node"], e["session"]))


def verify(events):
    sends = {e["message_id"]: e for e in events if e["event"] == "SEND"}
    receives = [e for e in events if e["event"] == "RECEIVE" and e.get("message_id") in sends]
    assert receives, "no peer send/receive pairs found"
    for event in receives:
        assert sends[event["message_id"]]["clock"] < event["clock"], event
    for event in events:
        if event["event"] in ("RECEIVE", "RECEIVE_REPLY"):
            assert event["clock"] > event["remote_clock"], event
    by_session = {}
    for event in events:
        key = (event["node"], event["session"])
        by_session.setdefault(key, []).append(event["clock"])
    for clocks in by_session.values():
        assert len(clocks) == len(set(clocks)), "duplicate logical timestamp in session"
    return {"events": len(events), "causal_message_pairs": len(receives)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("folder", nargs="?", default="logs/coordination")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    events = ordered(args.folder)
    if args.check:
        print(json.dumps(verify(events)))
    else:
        for event in events:
            print(json.dumps(event, sort_keys=True))


if __name__ == "__main__":
    main()
