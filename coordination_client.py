"""Inspect coordination or request a booking through any existing driver peer."""
import argparse
import json
import uuid
from peer_overlay import call, endpoint


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("address", type=endpoint)
    parser.add_argument("command", choices=("status", "reserve"))
    parser.add_argument("--resource", default="D1")
    parser.add_argument("--rider", default="rider-demo")
    parser.add_argument("--request-id", default=None)
    parser.add_argument("--hold", type=float, default=0)
    args = parser.parse_args()
    message = {"op": "C_STATUS"} if args.command == "status" else {
        "op": "C_RESERVE", "resource": args.resource, "rider": args.rider,
        "request_id": args.request_id or uuid.uuid4().hex, "hold": args.hold}
    result = call(args.address, message, timeout=15)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
