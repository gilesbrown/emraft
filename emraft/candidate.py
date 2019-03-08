from .starts_election import StartsElection
from .rpc import RequestVote, RequestVoteResponse


def request_vote_rpc(server):
    request_vote = RequestVote(term=server.current_term,
                               candidate_id=server.network.id,
                               last_log=server.log.last())
    return request_vote


def self_vote_rpc(server):
    server.voted_for = server.network.id
    grant_vote = RequestVoteResponse(
        term=server.current_term,
        sender=server.network.id,
        vote_granted=True,
    )
    return grant_vote


class Candidate(StartsElection):
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
        # TODO: beautify this
        server.receive(self_vote_rpc(server))
        server.network.send(request_vote_rpc(server))

    def vote(self, server, vote_granted, voter):
        """ Recieved response to RequestVote from `voter` """
        if vote_granted:
            self.votes.add(voter)
            if server.network.majority(self.votes):
                server.change_state(server.Leader(server))

    def append_entries(self, server, *args, **kwargs):
        """ Called when an AppendEntries RPC is received.

            • If AppendEntries RPC received from new leader: convert to
            follower
        """
        server.change_state(server.Follower(server))
        return server.state.append_entries(server, *args, **kwargs)
