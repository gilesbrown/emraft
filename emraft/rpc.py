import logging

Term = int
LogIndex = int
NetworkId = lambda x: x
VoteGranted = bool
Success = bool


def log_key(pair):
    if len(pair) != 2:
        raise ValueError()
    return (Term(pair[0]), LogIndex(pair[1]))


LogKey = log_key


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

    def method(self, state, name):
        return getattr(state, name, MissingMethod(name))


class RequestVoteResponse(RPC):

    def __init__(self, term, sender, vote_granted):
        super(RequestVoteResponse, self).__init__(term, sender)
        self.vote_granted = VoteGranted(vote_granted)

    def __call__(self, server):
        vote = self.method(server.state, 'vote')
        vote(server, vote_granted=self.vote_granted, voter=self.sender)


class RequestVote(RPC):

    def __init__(self, term, candidate_id, last_log):
        super(RequestVote, self).__init__(term, candidate_id)
        self.candidate_id = self.sender  # alias
        self.last_log = LogKey(last_log)

    def __call__(self, server):
        method = self.method(server.state, 'request_vote')
        vote_granted = method(server,
                              term=self.term,
                              candidate_id=self.candidate_id,
                              last_log=self.last_log)
        return RequestVoteResponse(term=server.current_term,
                                   vote_granted=vote_granted,
                                   sender=server.network.id)


class AppendEntriesResponse(RPC):

    def __init__(self, term, sender, success):
        super(AppendEntriesResponse, self).__init__(term, sender)
        self.success = Success(success)

    def __call__(self, server):
        entries_appended = self.method(server.state, 'entries_appended')
        entries_appended(server, success=self.success)


class AppendEntries(RPC):

    def __init__(self,
                 term,
                 leader_id,
                 prev_log,
                 entries,
                 leader_commit):
        super(AppendEntries, self).__init__(term, leader_id)
        self.leader_id = self.sender  # alias
        self.prev_log = LogKey(prev_log)
        self.entries = entries
        self.leader_commit = leader_commit

    def __call__(self, server):
        append_entries = self.method(server.state, 'append_entries')
        success = append_entries(server,
                                 term=self.term,
                                 leader_id=self.leader_id,
                                 prev_log=self.prev_log,
                                 entries=self.entries,
                                 leader_commit=self.leader_commit)
        return AppendEntriesResponse(term=server.current_term,
                                     sender=server.network.id,
                                     success=success)
