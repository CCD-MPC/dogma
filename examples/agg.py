import sys
import json

from dogma.verify import Verify
from conclave.utils import *
from conclave.lang import *


def protocol():

    cols_in_a = [
        defCol('a', 'INTEGER', [1]),
        defCol('b', 'INTEGER', [1]),
        defCol('c', 'INTEGER', [1]),
    ]
    cols_in_b = [
        defCol('a', 'INTEGER', [2]),
        defCol('b', 'INTEGER', [2]),
        defCol('c', 'INTEGER', [2]),
    ]

    in1 = create("in1", cols_in_a, {1})
    in2 = create("in2", cols_in_b, {2})

    proj1 = project(in1, "proj1", ['a', 'b'])
    proj2 = project(in2, "proj2", ['a', 'b'])

    cc1 = concat([proj1, proj2], 'cc1', ['d', 'e'])

    agg1 = aggregate(cc1, "agg1", ['d'], "e", "mean", "meanCol")

    collect(agg1, 1)

    return {in1, in2}


if __name__ == "__main__":

    with open(sys.argv[1], 'r') as c:
        conf = json.load(c)
    with open(sys.argv[2], 'r') as p:
        policy = json.load(p)

    ver = Verify(protocol, policy, conf)
    t = ver.verify()

    print(t)
