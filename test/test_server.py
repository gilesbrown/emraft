from emraft.network import Network
from emraft.server import Server
from emraft.rpc import (RequestVote,
                        RequestVoteResponse,
                        AppendEntries)


class Log:
    """ LOG """

    def __init__(self):
        self.log = [(0, None)]

    def last(self):
        log_index = len(self.log) - 1
        return (log_index, self.log[log_index][0])


class MockPersistentState:
    """ Fake """

    def __init__(self, current_term=0, voted_for=None):
        self._current_term = current_term
        self.voted_for = voted_for
        self.log = Log()

    def set_current_term(self, term):
        self._current_term = term

    def get_current_term(self):
        return self._current_term


class StopServer(Exception):
    """ Raised when we want to stop the Server """


def stop_server(server):
    raise StopServer()


class MockNetwork(Network):

    def __init__(self, servers=[1], election_timeout=0.15):
        super(MockNetwork, self).__init__()
        self.servers = servers
        self.id = self.servers[-1]
        self._election_timeout = election_timeout

    def election_timeout(self):
        return self._election_timeout

    def __len__(self):
        return len(self.servers)

    def send(self, rpc, dst=Network.ALL):
        pass


def test_server_becomes_singleton_leader():
    """ Test server becomes leader in singleton network """

    network = MockNetwork()
    persistent_state = MockPersistentState()

    server = Server(persistent_state, network)
    server.after(0.6, stop_server)
    try:
        server.scheduler.run()
    except StopServer:
        pass
    assert isinstance(server.state, server.Leader)


def test_server_becomes_leader():

    sends = []

    class GrantVotesNetwork(MockNetwork):

        def send(self, rpc, dst=Network.ALL):
            sends.append((rpc, dst))
            if isinstance(rpc, RequestVote):
                for server in self.servers[:-1]:
                    response = RequestVoteResponse(
                        self.server.current_term,
                        server,
                        vote_granted=True)
                    self.server.after(0, Server.receive, rpc=response)
            elif isinstance(rpc, AppendEntries):
                self.server.after(0, stop_server)
            else:
                print("WHAT?", rpc)

    network = GrantVotesNetwork([1, 2, 3])
    persistent_state = MockPersistentState()

    server = Server(persistent_state, network)
    try:
        server.scheduler.run()
    except StopServer:
        pass
    assert isinstance(server.state, server.Leader)
    assert len(sends) == 2


def Xtest_server_stopped():
    network = MockNetwork()
    persistent_state = MockPersistentState()
    server = Server(persistent_state, network)
    server.after(0.02, stop_server)
    try:
        server.scheduler.run()
        assert False
    except StopServer:
        pass
    assert isinstance(server.state, server.Follower)
