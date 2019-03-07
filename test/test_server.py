from emraft.network import Network
from emraft.server import Server
from emraft.rpc import (RequestVote,
                        RequestVoteResponse,
                        AppendEntries,
                        AppendEntriesResponse)


class MemLog:
    """ In-memory Raft log (for testing) """

    def __init__(self):
        self.log = [(0, None)]

    def get_term(self, index):
        if index < len(self.log):
            return self.log[index][0]

    def append(self, entries):
        pass

    def last(self):
        log_index = len(self.log) - 1
        return (log_index, self.log[log_index][0])


class MemPersistentState:
    """ In-memory "persistent" server state (for testing) """

    def __init__(self, current_term=0, voted_for=None):
        self._current_term = current_term
        self.voted_for = voted_for
        self.log = MemLog()

    def set_current_term(self, term):
        self._current_term = term

    def get_current_term(self):
        return self._current_term


class StopServer(Exception):
    """ Raised when we want to stop the Server """


def stop_server(server):
    raise StopServer()


class SimulatedNetwork(Network):

    def __init__(self, servers):
        super(SimulatedNetwork, self).__init__()
        self.servers = servers
        self.server = None
        # *this* server is the last in the list
        self.id = self.servers[-1]
        self.sends = []

    def __len__(self):
        return len(self.servers)

    def request_vote(self, request, server, grant_vote):
        resp = RequestVoteResponse(term=self.server.current_term,
                                   sender=server,
                                   vote_granted=grant_vote)
        self.server.after(0, Server.receive, rpc=resp)

    def send(self, rpc, dst=Network.ALL):
        # Stop when server first sends "AppendEntries".
        # asendsbecomes leader
        if isinstance(rpc, AppendEntries):
            self.server.after(0, stop_server)


class GrantVotesNetwork(SimulatedNetwork):
    """ Simulate votes being granted """

    def send(self, rpc, dst=Network.ALL):
        self.sends.append((rpc, dst))
        if isinstance(rpc, RequestVote):
            for server in self.servers[:-1]:
                self.request_vote(rpc, server, True)
        elif isinstance(rpc, AppendEntries):
            self.server.after(0, stop_server)
        else:
            assert False, "unexpected rpc {!r}".format(rpc)


class DontGrantVotesNetwork(SimulatedNetwork):
    """ Simulate votes not being granted """

    def on_request_vote(self):
        pass

    def send(self, rpc, dst=Network.ALL):
        self.sends.append((rpc, dst))
        if isinstance(rpc, RequestVote):
            for server in self.servers[:-1]:
                self.request_vote(rpc, server, False)
            self.on_request_vote()
        elif isinstance(rpc, AppendEntriesResponse):
            # we will send one of these wne we receive an AppendEntries
            pass
        else:
            assert False, "unexpected rpc {!r}".format(rpc)


class ReceiveAppendEntriesNetwork(DontGrantVotesNetwork):

    def on_request_vote(self):
        append_entries = AppendEntries(term=self.server.current_term + 1,
                                       leader_id=self.servers[-2],
                                       prev_log_index=0,
                                       prev_log_term=0,
                                       entries=[],
                                       leader_commit=0)
        self.server.receive(append_entries)
        self.server.after(0.1, stop_server)


def run_server(network, timeout=10.0):
    persistent_state = MemPersistentState()
    network.server = Server(persistent_state, network)
    # add an overall timeout for the run
    network.server.after(timeout, stop_server)
    try:
        network.server.scheduler.run()
    except StopServer:
        pass
    return network.server


def test_server_becomes_singleton_leader():
    """ Test server becomes leader in singleton network """
    server = run_server(SimulatedNetwork([7]))
    assert isinstance(server.state, server.Leader)


def test_server_remains_candidate_no_majority():
    """ Test server remains candidate in size 2 network """
    server = run_server(SimulatedNetwork([1, 2]), timeout=1.0)
    assert isinstance(server.state, server.Candidate)


def test_server_becomes_leader():
    server = run_server(GrantVotesNetwork([1, 2, 3]))
    assert isinstance(server.state, server.Leader)
    assert len(server.network.sends) == 2


def test_server_looses_election():
    """ Test server looses election """
    server = run_server(DontGrantVotesNetwork([1, 2, 3]), timeout=1.0)
    assert isinstance(server.state, server.Candidate)


def test_server_looses_election_another_wins():
    """ Test server looses election """
    server = run_server(ReceiveAppendEntriesNetwork([1, 2, 3]), timeout=1.0)
    assert isinstance(server.state, server.Follower)
