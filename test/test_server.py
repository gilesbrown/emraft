from emraft.network import Network
from emraft.server import Server
from emraft.rpc import (RequestVote,
                        RequestVoteResponse,
                        AppendEntries)


class MemLog:
    """ In-memory Raft log (for testing) """

    def __init__(self):
        self.log = [(0, None)]

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
        # *this* server is the last in the list
        self.id = self.servers[-1]
        self.sends = []

    def __len__(self):
        return len(self.servers)

    def send(self, rpc, dst=Network.ALL):
        # Stop when server first sends "AppendEntries".
        # asendsbecomes leader
        if isinstance(rpc, AppendEntries):
            self.server.after(0, stop_server)


class GrantVotesNetwork(SimulatedNetwork):
    """ Simulate votes being granted """

    def vote(self, request, server):
        rpc = RequestVoteResponse(term=self.server.current_term,
                                  sender=server,
                                  vote_granted=True)
        self.server.after(0, Server.receive, rpc=rpc)

    def send(self, rpc, dst=Network.ALL):
        self.sends.append((rpc, dst))
        if isinstance(rpc, RequestVote):
            for server in self.servers[:-1]:
                self.vote(rpc, server)
        elif isinstance(rpc, AppendEntries):
            self.server.after(0, stop_server)
        else:
            assert False, "unexpected rpc {!r}".format(rpc)


def run_server(network, timeout=10.0):
    persistent_state = MemPersistentState()
    server = Server(persistent_state, network)
    # add an overall timeout for the run
    server.after(timeout, stop_server)
    try:
        server.scheduler.run()
    except StopServer:
        pass
    return server


def test_server_becomes_singleton_leader():
    """ Test server becomes leader in singleton network """
    server = run_server(SimulatedNetwork([7]))
    assert isinstance(server.state, server.Leader)


def test_server_becomes_leader():

    server = run_server(GrantVotesNetwork([1, 2, 3]))
    assert isinstance(server.state, server.Leader)
    assert len(server.network.sends) == 2


def test_server_remains_candidate():
    """ Test server remains candidate in size 2 network """
    server = run_server(SimulatedNetwork([1, 2]), timeout=1.0)
    assert isinstance(server.state, server.Candidate)
