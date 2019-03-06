import logging
import sched
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

    def __str__(self):
        fields = dict(
            state=self.state.__class__.__name__[0],
            term=self.current_term,
            id=self.network.id,
        )
        return '{state}[{term}]@{id}'.format(**fields)

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

    def _receive(self, rpc):
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

    def receive(self, rpc):
        self.scheduler.enter(0, PRIORITY, self._receive, (rpc,))

    def after(self, delay, action, **kwargs):
        self.scheduler.enter(delay, PRIORITY, action, (self,), kwargs)

    def after_election_timeout(self, action):
        self.after(self.network.election_timeout(), action)
