class StartsElection:
    """ Base class for Follower and Candidate states.

    Both Follower and Candidate states transition to
    Candidate state after an election timeout
    triggering a new election.
    """

    timer = None

    def __init__(self, server):
        self.start_election_timer(server)

    def start_election_timer(self, server):
        if self.timer is not None:
            server.scheduler.cancel(self.timer)
        self.timer = server.after_election_timeout(self.start_election)

    def start_election(self, server):
        if server.state is self:
            server.change_state(server.Candidate(server))
