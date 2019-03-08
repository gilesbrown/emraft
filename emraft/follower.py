from .starts_election import StartsElection
from .rpc import RequestVoteResponse


class Follower(StartsElection):
    """
        Followers (§5.2):
          • Respond to RPCs from candidates and leaders
          • If election timeout elapses without receiving AppendEntries
            RPC from current leader or granting vote to candidate:
            convert to candidate
    """

    def request_vote(self,
                     server,
                     term,
                     candidate_id,
                     last_log):
        """
          Receiver implementation:
            1. Reply false if term < currentTerm (§5.1)
            2. If votedFor is null or candidateId, and candidate’s log is at
               least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        """
        # remember: current_term = term if term > current_term
        assert term <= server.current_term
        if (term == server.current_term
            and server.voted_for in (None, candidate_id)
            and server.log.last() <= last_log):
            vote_granted = True
        else:
            vote_granted = False
        response = RequestVoteResponse(term=server.current_term,
                                       vote_granted=vote_granted,
                                       sender=server.network.id)
        return response

    def append_entries(self,
                       server,
                       term,
                       leader_id,
                       prev_log,
                       entries,
                       leader_commit):
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
        self.start_election_timer(server)

        # 1. Reply false if term < currentTerm (§5.1)
        if term < server.current_term:
            return False

        log_term = server.log.get_term(prev_log[1])
        if log_term is None:
            # 2. Reply false if log doesn’t contain an entry at prevLogIndex
            #    whose term matches prevLogTerm (§5.3)
            return False

        if log_term != prev_log[0]:
            # 3. If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it (§5.3)
            server.log.delete_from(prev_log[1])
            return False

        # 4. Append any new entries not already in the log
        server.log.append(entries)

        #  5. If leaderCommit > commitIndex, set commitIndex =
        #     min(leaderCommit, index of last new entry)

        return True
