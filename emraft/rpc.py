import logging


Term = int
LogIndex = int
NetworkId = str
VoteGranted = bool

logger = logging.getLogger(__name__)


class MissingMethod:

    def __init__(self, name):
        self.name = name

    def __call__(self, server, **kwargs):
        logger.warning("no method '%s' on %s", self.name, server)


class RPC:

    def __init__(self, term, sender):
        self.term = Term(term)
        self.sender = NetworkId(sender)

    def __call__(self, server):
        raise NotImplementedError()

    def state_method(self, state, name):
        return getattr(state, name, MissingMethod(name))


class RequestVoteResponse(RPC):

    def __init__(self, term, sender, vote_granted):
        super(RequestVoteResponse, self).__init__(term, sender)
        self.vote_granted = VoteGranted(vote_granted)

    def __call__(self, server):
        vote = self.state_method(server.state, 'vote')
        vote(server, vote_granted=self.vote_granted, voter=self.sender)


class RequestVote(RPC):

    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        super(RequestVote, self).__init__(term, candidate_id)
        self.candidate_id = self.sender  # alias
        self.last_log_index = LogIndex(last_log_index)
        self.last_log_term = Term(last_log_term)

    def __call__(self, state):
        method = getattr(state, 'request_vote')
        vote_granted = method(self.candidate_id,
                              self.last_log_index,
                              self.last_log_term)
        return RequestVoteResponse(self.server.current_term, vote_granted)


class AppendEntries(RPC):

    def __init__(self,
                 term,
                 leader_id,
                 prev_log_index,
                 prev_log_term,
                 entries,
                 leader_commit):
        super(AppendEntries, self).__init__(term, leader_id)
        self.leader_id = self.sender  # alias
        self.prev_log_index = LogIndex(prev_log_index)
        self.prev_log_term = Term(prev_log_term)
        self.entries = entries
        self.leader_commit = leader_commit

    def __call__(self, state):
        method = getattr(state, 'request_vote', None)
        if method:
            print("TODO:", __file__)
