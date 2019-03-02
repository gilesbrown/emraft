from .start_election import StartElection
from ..rpc import RequestVote, RequestVoteResponse


def request_vote(server):
    last_log_index, last_log_term = server.log.last()
    request_vote = RequestVote(term=server.current_term,
                               candidate_id=server.network.id,
                               last_log_index=last_log_index,
                               last_log_term=last_log_term)
    return request_vote


def own_vote(server):
    server.voted_for = server.network.id
    grant_vote = RequestVoteResponse(
        term=server.current_term,
        sender=server.network.id,
        vote_granted=True,
    )
    return grant_vote


class Candidate(StartElection):
    """
    Candidates (§5.2):
      • On conversion to candidate, start election:
      • Increment currentTerm
      • Vote for self
      • Reset election timer
      • Send RequestVote RPCs to all other servers
      • If votes received from majority of servers: become leader
      • If AppendEntries RPC received from new leader: convert to follower
      • If election timeout elapses: start new election
    """

    def __init__(self, server):
        super(Candidate, self).__init__(server)
        self.votes = set()
        server.current_term += 1
        server.voted_for = server.network.id
        server.network.send(request_vote(server))
        server.receive(own_vote(server))

    def vote_granted(self, server, vote_from):
        self.votes.add(vote_from)
        if server.network.majority(self.votes):
            server.change_state(server.Leader(server))

    def append_entries(self, server, *args, **kwargs):
        print("TODO:", __file__)
