import asyncio
import functools
import pickle


class IAMMsg:
    """ Message identifying peer. """

    def __init__(self, pid: str):
        self.pid = pid

    def __str__(self):
        return "IAMMsg({})".format(self.pid)


class PolicyMsg:
    """ Message specifying a policy corresponding to an input party. """

    def __init__(self, pid: int, policy: str):
        self.pid = pid
        self.policy = policy

    def __str__(self):
        return "PolicyMsg({})".format(self.pid)


class PolicyProtocolClient(asyncio.Protocol):
    """
    The Salmon network protocol defines what messages salmon
    peers can send each other and how to interpret them.
    """

    def __init__(self, peer, loop):
        """ Initialize SalmonProtocol object. """

        self.peer = peer
        self.loop = loop
        self.transport = None

    def connection_made(self, transport):

        self.transport = transport

    def data_received(self, data):

        msg = pickle.loads(data)

        if isinstance(msg, IAMMsg):
            print("IAMMsg received: {}".format(msg.pid))
        elif isinstance(msg, PolicyMsg):
            print("PolicyMsg received: {}".format(msg.pid))

            policy = self.peer.policies[msg.pid]
            if isinstance(policy, asyncio.Future):
                policy.set_result(msg.policy)

            # self.peer.policies[msg.pid] = msg.policy
        else:
            print("Weird message: {}".format(msg))

    def connection_lost(self, exc):
        print('The server closed the connection')


class PolicyProtocolServer(asyncio.Protocol):

    def __init__(self, peer):

        self.peer = peer
        self.transport = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))

        self.transport = transport

    def data_received(self, data):

        msg = pickle.loads(data)

        if isinstance(msg, IAMMsg):

            conn = self.peer.peer_connections[msg.pid]
            if isinstance(conn, asyncio.Future):
                conn.set_result((self.transport, self))

                print("IAMMsg received: {}".format(msg.pid))
                self._handle_iam_msg()

        elif isinstance(msg, PolicyMsg):

            policy = self.peer.policies[msg.pid]
            if isinstance(policy, asyncio.Future):
                policy.set_result(msg.policy)

                print("PolicyMsg received: {}".format(msg.pid))
                self._handle_policy_msg()

        else:
            print("Weird message: {}".format(msg))

    def _handle_iam_msg(self):

        msg = IAMMsg(self.peer.pid)
        self.transport.write(pickle.dumps(msg))

    def _handle_policy_msg(self):

        msg = PolicyMsg(self.peer.pid, "POLICY")
        self.transport.write(pickle.dumps(msg))


class PolicyPeer:
    """
    A salmon network peer exposes networking functionality. Used to transfer
    messages to other peers and forward the received messages to the other peers.
    """

    def __init__(self, loop, config):

        self.pid = config["pid"]
        self.parties = config["parties"]
        self.host = self.parties[self.pid]["host"]
        self.port = self.parties[self.pid]["port"]
        self.peer_connections = {}
        self.loop = loop
        self.server = self.setup_server()
        self.policies = self.setup_policy_dict()

    def setup_server(self):

        return self.loop.create_server(lambda: PolicyProtocolServer(self), host=self.host, port=self.port)

    def setup_policy_dict(self):

        ret = {}

        for other_pid in self.parties.keys():
            if other_pid != self.pid:
                ret[other_pid] = asyncio.Future()

        return ret

    def close_server(self):

        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    def _send_iam(self, conn):

        msg = IAMMsg(self.pid)
        formatted = pickle.dumps(msg)
        transport, protocol = conn.result()
        transport.write(formatted)

    def _send_policy(self, conn):

        msg = PolicyMsg(self.pid, 'POLICY')
        formatted = pickle.dumps(msg)
        conn.write(formatted)

    async def _create_connection(self, f, other_host, other_port):

        while True:
            try:
                conn = await self.loop.create_connection(f, other_host, other_port)
                return conn
            except OSError:
                print("Retrying connection to {} {}".format(other_host, other_port))
                await asyncio.sleep(1)

    def connect_to_others(self):

        to_wait_on = []

        for other_pid in self.parties.keys():
            if int(other_pid) < int(self.pid):

                print("Will connect to {}".format(other_pid))

                conn = asyncio.ensure_future(
                    self._create_connection(
                        lambda: PolicyProtocolClient(self, self.loop),
                        self.parties[other_pid]["host"],
                        self.parties[other_pid]["port"]))

                self.peer_connections[other_pid] = conn
                conn.add_done_callback(functools.partial(self._send_iam))
                to_wait_on.append(conn)

            elif int(other_pid) > int(self.pid):

                print("Will wait for {} to connect".format(other_pid))
                connection_made = asyncio.Future()
                self.peer_connections[other_pid] = connection_made
                to_wait_on.append(connection_made)

        self.loop.run_until_complete(asyncio.gather(*to_wait_on))

        for pid in self.peer_connections:
            completed_future = self.peer_connections[pid]
            self.peer_connections[pid] = completed_future.result()[0]

    def exchange_policies(self):

        to_wait_on = []

        for other_pid in self.parties.keys():
            if int(other_pid) < int(self.pid):

                print("Sending policy to {}".format(other_pid))
                self._send_policy(self.peer_connections[other_pid])
                to_wait_on.append(self.policies[other_pid])

            elif int(other_pid) > int(self.pid):

                print("Will wait for policy from {}".format(other_pid))
                to_wait_on.append(self.policies[other_pid])

        self.loop.run_until_complete(asyncio.gather(*to_wait_on))

        for pid in self.policies.keys():
            if pid != self.pid:
                completed_future = self.policies[pid]
                self.policies[pid] = completed_future.result()


def setup_peer(config):
    """
    Creates a peer and connects peer to all other peers. Blocks until connection succeeds.
    """

    loop = asyncio.get_event_loop()
    peer = PolicyPeer(loop, config)
    peer.server = loop.run_until_complete(peer.server)
    peer.connect_to_others()
    peer.exchange_policies()

    return peer
