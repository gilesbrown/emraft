from collections import Counter
import pytest
from emraft.rpc import (RequestVote,
                        RequestVoteResponse,
                        AppendEntries,
                        AppendEntriesResponse)
from emraft.persistent_state import SQLitePersistentState

from simulated_network import (SimulatedNetwork,
                               VotingNetwork,
                               run_server)


@pytest.fixture
def persistent_state(request):
    return SQLitePersistentState.connect(':memory:')


def test_server_becomes_leader_in_singleton_network(persistent_state):

    class SingletonNetwork(SimulatedNetwork):

        def send_RequestVote(self, rpc, dst):
            assert dst is self.ALL

        def send_AppendEntries(self, rpc, dst):
            assert dst is self.ALL
            self.server.after(0, self.stop_server)

    server = run_server(persistent_state, SingletonNetwork([7]))
    assert isinstance(server.state, server.Leader)


def test_server_grants_vote(persistent_state):

    class RequestVoteResponseNetwork(SimulatedNetwork):

        def send_RequestVoteResponse(self, rpc, dst):
            # server has sent response - end of test
            self.server.after(0, self.stop_server)

    def init_server(server):
        request_vote = RequestVote(
            term=server.current_term,
            candidate_id=server.network.servers[0],
            last_log=(0, 0)
        )
        server.receive(request_vote)

    server = run_server(persistent_state,
                        RequestVoteResponseNetwork([1, 2, 3]),
                        init_server=init_server,
                        timeout=0.5)
    assert len(server.network.sends) == 1
    assert isinstance(server.network.sends[0][0], RequestVoteResponse)
    assert server.network.sends[0][0].vote_granted
    assert isinstance(server.state, server.Follower)


def test_server_grants_vote_to_higher_term_candidate(persistent_state):

    class HigherTermCandidateNetwork(SimulatedNetwork):

        def send_RequestVote(self, rpc, dst):
            assert dst is self.ALL
            assert len(self.servers) > 1
            request_vote = RequestVote(
                term=self.server.current_term + 1,
                candidate_id=self.servers[0],
                last_log=(0, 0)
            )
            self.server.receive(request_vote)

        def send_RequestVoteResponse(self, rpc, dst):
            # server has sent response - end of test
            self.server.after(0, self.stop_server)

    server = run_server(persistent_state,
                        HigherTermCandidateNetwork([1, 2, 3]),
                        timeout=0.5)
    assert len(server.network.sends) == 2
    assert isinstance(server.network.sends[0][0], RequestVote)
    assert isinstance(server.network.sends[1][0], RequestVoteResponse)
    assert server.network.sends[1][0].vote_granted
    assert isinstance(server.state, server.Follower)


def test_server_remains_candidate_no_majority(persistent_state):
    """ Test server remains candidate in size 2 network """
    server = run_server(persistent_state,
                        VotingNetwork([1, 2], grant_vote=False),
                        timeout=0.5)
    assert isinstance(server.state, server.Candidate)


def test_server_becomes_leader(persistent_state):

    class BecomeLeaderNetwork(VotingNetwork):

        def send_AppendEntries(self, rpc, dst):
            # server has become leader - end of test
            self.server.after(0, self.stop_server)

    network = BecomeLeaderNetwork([1, 2, 3], grant_vote=True)
    server = run_server(persistent_state, network)
    assert isinstance(server.state, server.Leader)
    assert len(server.network.sends) == 2


def test_server_remains_candidate_when_no_response(persistent_state):
    """ Test server looses election """
    server = run_server(persistent_state,
                        VotingNetwork([1, 2, 3], grant_vote=False),
                        timeout=0.5)
    assert isinstance(server.state, server.Candidate)
    assert len(server.network.sends) == 3
    assert all(rpc for rpc, _ in server.network.sends)


def test_candidate_becomes_follower_after_heartbeat(persistent_state):

    class HeartbeatNetwork(SimulatedNetwork):

        def send_RequestVote(self, rpc, dst):
            append_entries = AppendEntries(term=self.server.current_term,
                                           leader_id=self.servers[-2],
                                           prev_log=(0, 0),
                                           entries=[],
                                           leader_commit=0)
            self.server.receive(append_entries)

        def send_AppendEntriesResponse(self, rpc, dst):
            # server has responsed to append entries - end of test
            self.server.after(0, self.stop_server)

    server = run_server(persistent_state,
                        HeartbeatNetwork([1, 2, 3]),
                        timeout=1.0)
    assert isinstance(server.state, server.Follower)
    assert len(server.network.sends) == 2
    assert isinstance(server.network.sends[0][0], RequestVote)
    assert isinstance(server.network.sends[1][0], AppendEntriesResponse)


def test_follower_remains_follower_when_receiving_heartbeats(persistent_state):

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

    class AppendEntriesResponseNetwork(SimulatedNetwork):

        def send_AppendEntriesResponse(self, rpc, dst):
            assert dst == self.servers[0]

    server = run_server(persistent_state,
                        AppendEntriesResponseNetwork([1, 2, 3]),
                        init_server=init_server,
                        timeout=0.5)

    assert isinstance(server.state, server.Follower)
    counts = Counter(rpc.__class__ for rpc, dst in server.network.sends)
    assert counts[AppendEntriesResponse] > 3
