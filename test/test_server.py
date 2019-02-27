from emraft.network import Network
from emraft.server import Server, Follower, Leader


class Log:
    """ LOG """

    def __init__(self):
        self.log = [(0, None)]

    def last(self):
        log_index = len(self.log) - 1
        return (log_index, self.log[log_index][0])


class PersistentState:
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


def stop_server():
    raise StopServer()


def test_server_becomes_leader():

    class SimulatedNetwork(Network):
        """ Pass """

        def send(self, rpc, dst=Network.ALL):
            pass

        def election_timeout(self):
            return 0.15  # 150ms

    network = SimulatedNetwork()
    persistent_state = PersistentState()

    server = Server(persistent_state, network)
    server.scheduler.run()
    assert isinstance(server.state, Leader)


def Xtest_server_stopped():
    server = Server()
    server.after(0.02, stop_server)
    try:
        server.scheduler.run()
        assert False
    except StopServer:
        pass
    assert isinstance(server.state, Follower)
