from collections import Counter
import pytest
from emraft.network import Network
from emraft.server import Server
from emraft.rpc import (RequestVote,
                        RequestVoteResponse,
                        AppendEntries,
                        AppendEntriesResponse)
from emraft.persistent_state import SQLitePersistentState

import mem_persistent_state

impl = ['mem', 'sqlite']


@pytest.fixture
def mem():
    return mem_persistent_state.MemPersistentState()


@pytest.fixture
def sqlite():
    return SQLitePersistentState.connect(':memory:')


@pytest.fixture
def persistent_state(request):
    return request.getfixturevalue(request.param)


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
        self.sends.append(rpc)
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
        elif isinstance(rpc, AppendEntriesResponse):
            pass
            # self.server.after(0, stop_server)
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
        append_entries = AppendEntries(term=self.server.current_term,
                                       leader_id=self.servers[-2],
                                       prev_log=(0, 0),
                                       entries=[],
                                       leader_commit=0)
        self.server.receive(append_entries)
        self.server.after(0.1, stop_server)


def run_server(persistent_state, network, timeout=10.0, init_server=None):
    # persistent_state = MemPersistentState()
    network.server = Server(persistent_state, network)
    # add an overall timeout for the run
    network.server.after(timeout, stop_server)
    if init_server:
        init_server(network.server)
    try:
        network.server.scheduler.run()
    except StopServer:
        pass
    return network.server


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_server_becomes_singleton_leader(persistent_state):
    """ Test server becomes leader in singleton network """
    server = run_server(persistent_state, SimulatedNetwork([7]))
    assert isinstance(server.state, server.Leader)


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_follower_grants_vote(persistent_state):
    def init_server(server):
        request_vote = RequestVote(
            term=1,
            candidate_id=server.network.servers[-2],
            last_log=(0, 0)
        )
        server.receive(request_vote)
    server = run_server(persistent_state,
                        SimulatedNetwork([7, 8]),
                        init_server=init_server,
                        timeout=0.1)
    assert len(server.network.sends) == 1
    assert isinstance(server.network.sends[0], RequestVoteResponse)
    assert server.network.sends[0].vote_granted
    assert isinstance(server.state, server.Follower)


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_server_remains_candidate_no_majority(persistent_state):
    """ Test server remains candidate in size 2 network """
    server = run_server(persistent_state,
                        SimulatedNetwork([1, 2]),
                        timeout=1.0)
    assert isinstance(server.state, server.Candidate)


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_server_becomes_leader(persistent_state):
    server = run_server(persistent_state, GrantVotesNetwork([1, 2, 3]))
    assert isinstance(server.state, server.Leader)
    assert len(server.network.sends) == 2


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_server_looses_election(persistent_state):
    """ Test server looses election """
    server = run_server(persistent_state,
                        DontGrantVotesNetwork([1, 2, 3]),
                        timeout=1.0)
    assert isinstance(server.state, server.Candidate)


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_candidate_receives_heartbeat(persistent_state):
    """ Test candidate switches to follower when heartbeat received """
    server = run_server(persistent_state,
                        ReceiveAppendEntriesNetwork([1, 2, 3]),
                        timeout=1.0)
    assert isinstance(server.state, server.Follower)


@pytest.mark.parametrize('persistent_state', impl, indirect=True)
def test_follower_receives_heartbeats(persistent_state):
    """ Test follower does not change state when receiving hearbeats """
    def init_server(server):
        def heartbeat(server):
            rpc = AppendEntries(
                term=server.current_term,
                leader_id=server.network.servers[0],
                prev_log=(0, 0),
                entries=[],
                leader_commit=server.commit_index
            )
            server.receive(rpc)
            delay = server.network.heartbeat_interval()
            server.after(delay, heartbeat)
        heartbeat(server)

    server = run_server(persistent_state,
                        GrantVotesNetwork([1, 2, 3]),
                        init_server=init_server,
                        timeout=0.5)

    assert isinstance(server.state, server.Follower)
    counts = Counter(rpc.__class__ for rpc, dst in server.network.sends)
    assert counts[AppendEntriesResponse] > 3
