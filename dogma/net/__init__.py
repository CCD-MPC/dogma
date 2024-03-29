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

    def __init__(self, pid: str, policy: dict):

        self.pid = pid
        self.policy = policy

    def __str__(self):
        return "PolicyMsg({})".format(self.pid)


class PolicyAckMsg:
    """ Message indicating that a party has received another party's policy. """

    def __init__(self, pid: str):

        self.pid = pid

    def __str__(self):
        return "PolicyReceivedMsg({})".format(self.pid)


class PolicyProtocolClient(asyncio.Protocol):
    """
    Network protocol for client side policy exchange.
    """

    def __init__(self, peer):

        self.peer = peer
        self.buffer = b""
        self.transport = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        print('Client connection made to: {}'.format(peername))

        self.transport = transport

    def data_received(self, data):
        """
        Add incoming messages to buffer and process them.
        """

        self.buffer += data
        self.handle_lines()

    def handle_lines(self):
        """
        Process messages in buffer.
        """

        while b"\n\n\n" in self.buffer:
            data, self.buffer = self.buffer.split(b"\n\n\n", 1)
            self.handle_msg(data)

    def handle_msg(self, data):
        """
        Determine message type and process accordingly.
        """

        msg = pickle.loads(data)

        if isinstance(msg, IAMMsg):
            self._handle_iam_msg(msg)

        elif isinstance(msg, PolicyMsg):
            self._handle_policy_msg(msg)

        elif isinstance(msg, PolicyAckMsg):
            self._handle_policy_ack_msg(msg)

        else:
            print("Weird message: {}".format(msg))

    @staticmethod
    def _handle_iam_msg(msg):

        print("IAMMsg received: {}".format(msg.pid))

    def _handle_policy_msg(self, msg):
        """
        Resolve policy future and ack it.
        """

        print("PolicyMsg received: {}".format(msg.pid))
        policy = self.peer.policies[msg.pid]["policy"]

        print("Sending PolicyAckMsg to: {}".format(msg.pid))
        self.peer.send_policy_ack(self.transport)

        if isinstance(policy, asyncio.Future):
            policy.set_result(msg.policy)

    def _handle_policy_ack_msg(self, msg):
        """
        Resolve policy ack future.
        """

        print("PolicyAckMsg received: {}".format(msg.pid))
        ack = self.peer.policies[msg.pid]["ack"]

        if isinstance(ack, asyncio.Future):
            ack.set_result(True)

    def connection_lost(self, exc):

        print('The server closed the connection')


class PolicyProtocolServer(asyncio.Protocol):
    """
    Network protocol for server side policy exchange.
    """

    def __init__(self, peer):

        self.peer = peer
        self.buffer = b""
        self.transport = None

    def connection_made(self, transport):

        peername = transport.get_extra_info('peername')
        print('Server connection made from: {}'.format(peername))

        self.transport = transport

    def data_received(self, data):
        """
        Add incoming messages to buffer and process them.
        """

        self.buffer += data
        self.handle_lines()

    def handle_lines(self):
        """
        Process messages in buffer.
        """

        while b"\n\n\n" in self.buffer:
            data, self.buffer = self.buffer.split(b"\n\n\n", 1)
            self.handle_msg(data)

    def handle_msg(self, data):
        """
        Determine message type and process accordingly.
        """

        msg = pickle.loads(data)

        if isinstance(msg, IAMMsg):
            self._handle_iam_msg(msg)

        elif isinstance(msg, PolicyMsg):
            self._handle_policy_msg(msg)

        elif isinstance(msg, PolicyAckMsg):
            self._handle_policy_ack_msg(msg)

        else:
            print("Weird message: {}".format(msg))

    def _handle_iam_msg(self, msg):
        """
        Respond to IAMMsg from client and resolve peer connection future.
        """

        print("IAMMsg received: {}".format(msg.pid))
        conn = self.peer.peer_connections[msg.pid]

        print("Sending IAMMsg to: {}".format(msg.pid))
        self.peer.send_iam(self.transport)

        if isinstance(conn, asyncio.Future):
            conn.set_result((self.transport, self))

    def _handle_policy_msg(self, msg):
        """
        Send ack for incoming policy, send policy, and resolve policy future.
        """

        print("PolicyMsg received: {}".format(msg.pid))
        policy = self.peer.policies[msg.pid]["policy"]

        print("Sending PolicyAckMsg to: {}".format(msg.pid))
        self.peer.send_policy_ack(self.transport)

        print("Sending PolicyMsg to: {}".format(msg.pid))
        self.peer.send_policy(self.transport)

        if isinstance(policy, asyncio.Future):
            policy.set_result(msg.policy)

    def _handle_policy_ack_msg(self, msg):
        """
        Resolve policy ack future.
        """

        print("PolicyAckMsg received: {}".format(msg.pid))
        ack = self.peer.policies[msg.pid]["ack"]

        if isinstance(ack, asyncio.Future):
            ack.set_result(True)


class PolicyPeer:
    """
    A salmon network peer exposes networking functionality. Used to transfer
    messages to other peers and forward the received messages to the other peers.
    """

    def __init__(self, loop, config, policy):

        self.pid = config["user_config"]["pid"]
        self.parties = self.setup_network_config(config)
        self.policy = policy
        self.host = self.parties[self.pid]["host"]
        self.port = self.parties[self.pid]["port"]
        self.peer_connections = {}
        self.loop = loop
        self.server = self.setup_server()
        self.policies = self.setup_futures_dict()

    def setup_server(self):

        return self.loop.create_server(lambda: PolicyProtocolServer(self), host=self.host, port=self.port)

    def setup_futures_dict(self):
        """
        Setup dictionary of Futures, where the keys correspond
        to the parties involved in the computation.
        """

        ret = {}

        for other_pid in self.parties.keys():
            if other_pid != self.pid:
                ret[other_pid] = {}
                ret[other_pid]["policy"] = asyncio.Future()
                ret[other_pid]["ack"] = asyncio.Future()

        return ret

    @staticmethod
    def setup_network_config(config):
        """
        Return network configuration dict.
        """

        ret = {}
        parties = config["net"]["parties"]

        for i in range(len(parties)):
            ret[i + 1] = {}
            ret[i + 1]["host"] = parties[i]["host"]
            ret[i + 1]["port"] = parties[i]["port"]

        return ret

    def close_server(self):

        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())

    def send_iam(self, conn):

        msg = IAMMsg(self.pid)
        formatted = pickle.dumps(msg) + b"\n\n\n"

        if isinstance(conn, asyncio.Future):
            transport, protocol = conn.result()
        else:
            transport = conn

        transport.write(formatted)

    def send_policy(self, conn):

        msg = PolicyMsg(self.pid, self.policy)
        formatted = pickle.dumps(msg) + b"\n\n\n"
        conn.write(formatted)

    def send_policy_ack(self, conn):

        msg = PolicyAckMsg(self.pid)
        formatted = pickle.dumps(msg) + b"\n\n\n"
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
                        lambda: PolicyProtocolClient(self),
                        self.parties[other_pid]["host"],
                        self.parties[other_pid]["port"]))

                self.peer_connections[other_pid] = conn
                conn.add_done_callback(functools.partial(self.send_iam))
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
        """
        Exchange policy with other parties and wait on acks.
        """

        to_wait_on = []

        for other_pid in self.parties.keys():

            if other_pid < self.pid:

                print("Sending policy to {}".format(other_pid))
                self.send_policy(self.peer_connections[other_pid])
                to_wait_on.append(self.policies[other_pid]["policy"])

            elif other_pid > self.pid:

                print("Will wait for policy from {}".format(other_pid))
                to_wait_on.append(self.policies[other_pid]["policy"])

        self.loop.run_until_complete(asyncio.gather(*to_wait_on))

        for pid in self.policies.keys():
            if pid != self.pid:
                completed_policy_future = self.policies[pid]["policy"]
                self.policies[pid]["policy"] = completed_policy_future.result()

        self._wait_on_acks()

    def _wait_on_acks(self):
        """
        Wait on acks from other parties indicating that they
        have received this party's policy.
        """

        to_wait_on = []

        for other_pid in self.parties.keys():

            if other_pid != self.pid:
                to_wait_on.append(self.policies[other_pid]["ack"])

        if len(to_wait_on) > 0:
            self.loop.run_until_complete(asyncio.gather(*to_wait_on))

        self.close_server()

        for pid in self.policies.keys():
            if pid != self.pid:
                completed_policy_ack_future = self.policies[pid]["ack"]
                self.policies[pid]["ack"] = completed_policy_ack_future.result()

    def get_policies_from_others(self):
        """
        Return all policies in dict.
        """

        if self.peer_connections == {}:
            self.connect_to_others()

        self.exchange_policies()

        ret = {self.pid: self.policy}

        for k in self.policies.keys():
            ret[k] = self.policies[k]["policy"]

        return ret


def setup_peer(config, policy):
    """
    Creates a peer and connects peer to all other peers. Blocks until connection succeeds.
    """

    loop = asyncio.get_event_loop()
    peer = PolicyPeer(loop, config, policy)
    peer.server = loop.run_until_complete(peer.server)
    peer.connect_to_others()

    return peer
