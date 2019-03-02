from .start_election import StartElection
from ..rpc import RequestVoteResponse


class Follower(StartElection):
    """
        Followers (§5.2):
          • Respond to RPCs from candidates and leaders
          • If election timeout elapses without receiving AppendEntries
            RPC from current leader or granting vote to candidate:
            convert to candidate
    """

    def request_vote(self, term, candidate_id, last_index, last_term):
        """
          Receiver implementation:
            1. Reply false if term < currentTerm (§5.1)
            2. If votedFor is null or candidateId, and candidate’s log is at
               least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        """
        # remember: current_term = term if term > current_term
        assert term <= self.server.current_term
        if (term == self.server.current_term
            and self.server.voted_for in (None or candidate_id)
            and self.server.log.up_to_date(last_index, last_term)):
            vote_granted = True
        else:
            vote_granted = False
        return RequestVoteResponse(vote_granted=vote_granted)

    def append_entries(self,
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

        # reset election timer
        self.start_election_timer()

        # 1. Reply false if term < currentTerm (§5.1)
        if term < self.server.current_term:
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
