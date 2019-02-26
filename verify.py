import sys

from src.policy_engine import PolicyEngine

a = PolicyEngine(sys.argv[1], sys.argv[2], sys.argv[3])
a.verify()

