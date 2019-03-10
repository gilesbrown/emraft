import pytest
from emraft.network import Network
from emraft.persistent_state import SQLitePersistentState
from emraft.server import Server
from emraft.follower import Follower


@pytest.fixture
def server(request):
    persistent_state = SQLitePersistentState.connect(':memory:')
    network = Network()
    server = Server(persistent_state, network)
    server.change_state(Follower(server))
    assert isinstance(server.state, Follower)
    return server


def test_request_vote_vote_granted(server):
    vote_granted = server.state.request_vote(server,
                                             term=server.current_term,
                                             candidate_id=999,
                                             last_log=(0, 0))
    assert vote_granted is True


def test_request_vote_not_granted_lower_term(server):
    server.current_term += 1
    vote_granted = server.state.request_vote(server,
                                             term=server.current_term - 1,
                                             candidate_id=999,
                                             last_log=(0, 0))
    assert vote_granted is False


def test_request_vote_not_granted_voted_for_other(server):
    server.voted_for = 888
    vote_granted = server.state.request_vote(server,
                                             term=server.current_term,
                                             candidate_id=999,
                                             last_log=(0, 0))
    assert vote_granted is False


def test_append_entries_lower_term(server):
    server.current_term += 1
    success = server.state.append_entries(server,
                                          term=server.current_term - 1,
                                          leader_id=999,
                                          prev_log=(0, 0),
                                          entries=[],
                                          leader_commit=0)
    assert success is False


def test_append_entries_no_log_entry(server):
    success = server.state.append_entries(server,
                                          term=server.current_term,
                                          leader_id=999,
                                          prev_log=(1, 1),
                                          entries=[],
                                          leader_commit=0)
    assert success is False


def test_append_entries_log_entry_wrong_term(server):
    success = server.state.append_entries(server,
                                          term=server.current_term,
                                          leader_id=999,
                                          prev_log=(1, 0),
                                          entries=[],
                                          leader_commit=0)
    assert success is False
