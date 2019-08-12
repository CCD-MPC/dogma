import policy_engine.net as pnet
import sys
import json

if __name__ == "__main__":

    p = pnet.setup_peer(json.load(open(sys.argv[1], 'r')))
    print(p.peer_connections)
    print(p.policies)
    print("\nDONE\n")