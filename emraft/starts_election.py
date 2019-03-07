class StartsElection:
    """ Base class for Follower and Candidate states.

    Both Follower and Candidate states transition to
    Candidate state after an election timeout
    triggering a new election.
    """

    election_timer = None

    def __init__(self, server):
        self.start_election_timer(server)

    def start_election_timer(self, server):
        if self.election_timer is not None:
            server.scheduler.cancel(self.timer)
        delay = server.network.election_timeout()
        self.election_timer = server.after(delay, self.start_election)

    def start_election(self, server):
        if server.state is self:
            server.change_state(server.Candidate(server))
