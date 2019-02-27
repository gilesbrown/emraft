

Term = int
LogIndex = int
NetworkId = str
VoteGranted = bool

class RPC:

    def __init__(self, term, sender):
        self.term = Term(term)
        self.sender = NetworkId(sender)

    def __call__(self, state):
        raise NotImplementedError()


class RequestVoteResponse(RPC):

    def __init__(self, term, sender, vote_granted):
        super(RequestVoteResponse, self).__init__(term, sender)
        self.vote_granted = VoteGranted(vote_granted)

    def __call__(self, state):
        if self.vote_granted:
            method = getattr(state, 'vote_granted', None)
            if method:
                method(self.sender)
            else:
                print("NO METHOD for vote_granted on {}".format(state))
        else:
            print("VOTE NOT GRANTED")


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
        return RequestVoteResponse(self.server.persistent_state.current_term,
                                   vote_granted)
