import logging
from . import follower, leader, candidate


PRIORITY = 0


class Server:
    """
    All Servers:
      • If commitIndex > lastApplied: increment lastApplied, apply
        log[lastApplied] to state machine (§5.3)
      • If RPC request or response contains term T > currentTerm:
        set currentTerm = T, convert to follower (§5.1)
    """

    Follower = follower.Follower
    Candidate = candidate.Candidate
    Leader = leader.Leader

    def __init__(self, persistent_state, network):
        self.logger = logging.getLogger(__name__)
        self.network = network
        self.network.receive = self.receive
        self.scheduler = network.scheduler()
        self.state = self.Follower(self)

        # persistent state on all servers
        self.persistent_state = persistent_state
        self.log = self.persistent_state.log

        # volatile state on all servers
        self.commit_index = 0
        self.last_applied = 0

    def __str__(self):
        fields = dict(
            state=self.state.__class__.__name__[0],
            term=self.current_term,
            id=self.network.id,
        )
        return '{state}[{term}]@{id}'.format(**fields)

    @property
    def current_term(self):
        return self.persistent_state.get_current_term()

    @current_term.setter
    def current_term(self, term):
        self.persistent_state.set_current_term(term)

    @property
    def voted_for(self):
        return self.persistent_state.get_voted_for()

    @voted_for.setter
    def voted_for(self, candidate):
        self.persistent_state.set_voted_for(candidate)

    def change_state(self, new_state):
        """ Change server state.

        This method is used for all state transitions defined
        in figure 4 in https://raft.github.io/raft.pdf apart
        from "starts up" including the Candidate -> Candidate
        transition.
        """
        self.state = new_state

    def receive_action(self, rpc):
        """
            • If RPC request or response contains term T > currentTerm:
                set currentTerm = T, convert to follower (§5.1)
        """
        if rpc.term > self.current_term:
            self.current_term = rpc.term
            self.change_state(self.Follower(self))
        response = rpc(self)
        if response is not None:
            self.network.send(response, rpc.sender)

    #
    # Scheduler helpers

    def receive(self, rpc):
        self.scheduler.enter(0, PRIORITY, self.receive_action, (rpc,))

    def after(self, delay, action, **kwargs):
        return self.scheduler.enter(delay, PRIORITY, action, (self,), kwargs)
