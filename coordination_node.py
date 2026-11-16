"""Run the existing driver peer overlay without the optional HTTP/broker stack."""
import argparse
import threading
from peer_overlay import PeerNode, endpoint


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("node")
    parser.add_argument("port", type=int)
    parser.add_argument("--seed", type=endpoint)
    args = parser.parse_args()
    peer = PeerNode(args.node, "127.0.0.1", args.port, "127.0.0.1",
                    args.port + 100, 37.77, -122.41,
                    lambda line: print(line, flush=True))
    try:
        if args.seed:
            peer.join(args.seed)
        threading.Event().wait()
    except KeyboardInterrupt:
        pass
    finally:
        peer.stop()


if __name__ == "__main__":
    main()
