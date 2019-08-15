import policy_engine.net as pnet
import sys
import json

if __name__ == "__main__":

    p = pnet.setup_peer(json.load(open(sys.argv[1], 'r')))
    policies = p.get_policies_from_others()
    print(policies)
    print("\nDONE\n")