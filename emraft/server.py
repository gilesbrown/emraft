import logging
import sched
from .rpc import (RequestVote, RequestVoteResponse)


class State:

    def __init__(self, server):
        self.server = server
        self.timer = None

    def become_candidate(self):
        if self.server.state is self:
            self.server.change_state(self.server.Candidate(self.server))


class Follower(State):
    """"
        Followers (§5.2):
          • Respond to RPCs from candidates and leaders
          • If election timeout elapses without receiving AppendEntries
            RPC from current leader or granting vote to candidate:
            convert to candidate
    """

    def __init__(self, server):
        super(Follower, self).__init__(server)
        self.timer = self.server.after(self.server.network.election_timeout(),
                                       self.become_candidate)

    def request_vote(self, term, candidate_id, last_index, last_term):
        """
          Receiver implementation:
            1. Reply false if term < currentTerm (§5.1)
            2. If votedFor is null or candidateId, and candidate’s log is at
               least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        """
        vote_granted = False
        # remember: current_term = term if term > current_term
        assert term <= self.server.current_term
        if (term == self.server.current_term
            and self.server.voted_for in (None or candidate_id)
            and self.server.log.up_to_date(last_index, last_term)):
            vote_granted = True
        return RequestVoteResponse(vote_granted=vote_granted)

    def request_append_entries(self,
                               term,
                               leader_id,
                               prev_log_index,
                               prev_log_term,
                               entries):
        """
            entries[] log entries to store (empty for heartbeat;
            may send more than one for efficiency)
            leaderCommit leader’s commitIndex
            Results:
            term currentTerm, for leader to update itself
            success true if follower contained entry matching
            prevLogIndex and prevLogTerm
        """

        # 1. Reply false if term < currentTerm (§5.1)
        if term < self.server.persistent_state.current_term:
            return False

        # 2. Reply false if log doesn’t contain an entry at prevLogIndex
        #    whose term matches prevLogTerm (§5.3)
        prev_log_entry = self.server.log.get(prev_log_index)
        if prev_log_entry is not None:
            # 3. If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it (§5.3)
            if prev_log_entry.term != prev_log_term:
                self.server.log.delete_from(prev_log_index)
                return False
        else:
            return False

        # 4. Append any new entries not already in the log
        self.server.log.append(entries)

        #  5. If leaderCommit > commitIndex, set commitIndex =
        #     min(leaderCommit, index of last new entry)
        return True


class Candidate(State):
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
        self.server.current_term += 1
        self.server.voted_for = self.server.network.id
        last_log_index, last_log_term = self.server.log.last()
        request_vote = RequestVote(term=server.current_term,
                                   candidate_id=server.network.id,
                                   last_log_index=last_log_index,
                                   last_log_term=last_log_term)

        self.server.network.send(request_vote)
        vote_for_self = RequestVoteResponse(
            term=server.current_term,
            sender=server.network.id,
            vote_granted=True,
        )
        self.server.after(0.0, self.server.invoke, rpc=vote_for_self)
        self.timer = self.server.after(self.server.network.election_timeout(),
                                       self.become_candidate)

    def vote_granted(self, voter):
        self.votes.add(voter)
        if self.server.network.majority(self.votes):
            self.server.change_state(self.server.Leader(self.server))


class Leader(State):
    """ pass """

    def response_append_entries(self, granted, server_id):
        if granted:
            self.votes.add(server_id)


PRIORITY = 0


class Server:
    """
    All Servers:
      • If commitIndex > lastApplied: increment lastApplied, apply
        log[lastApplied] to state machine (§5.3)
      • If RPC request or response contains term T > currentTerm:
        set currentTerm = T, convert to follower (§5.1)
    """

    Follower = Follower
    Candidate = Candidate
    Leader = Leader

    def __init__(self, persistent_state, network):
        self.logger = logging.getLogger(__name__)
        self.network = network
        self.network.bind(self)
        self.scheduler = sched.scheduler(self.network.timefunc,
                                         self.network.delayfunc)
        self.state = self.Follower(self)

        # persistent state on all servers
        self.persistent_state = persistent_state
        self.log = self.persistent_state.log

        self._current_term = None

        # volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0

    @property
    def current_term(self):
        if self._current_term is None:
            self._current_term = self.persistent_state.get_current_term()
        return self._current_term

    @current_term.setter
    def current_term(self, term):
        self.persistent_state.set_current_term(term)
        self._current_term = None
        self._voted_for = None

    def change_state(self, new_state):
        self.state = new_state

    def invoke(self, rpc):
        """
            • If RPC request or response contains term T > currentTerm:
                set currentTerm = T, convert to follower (§5.1)
        """
        if rpc.term > self.current_term:
            self.current_term = rpc.term
            self.become_follower()
        response = rpc(self.state)
        if response is not None:
            self.network.send(response, rpc.sender)

    def after(self, delay, action, args=(), **kwargs):
        self.scheduler.enter(delay, PRIORITY, action, args, kwargs)
