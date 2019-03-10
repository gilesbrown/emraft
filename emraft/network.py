import time
import sched


class Network(object):
    """ Network abstraction used by our emraft.server.Server class.

    A real Network subclass will need to:
      * implement `send`
      * implement `__len__`
      * callback to `self.receive`
      * manage configuration

    The recommended timeout from the Raft PDF is:
        broadcastTime ≪ electionTimeout ≪ MTBF
    """

    ALL = object()

    def __init__(self, **kw):
        self.id = id(self)
        self.receive = None
        self.timeout = kw.pop('timeout', 0.15)
        self.majority_threshold = kw.pop('majority_threshold', 0.5)

    def scheduler(self):
        """ Return a scheduler for scheduling server actions.
        """
        return sched.scheduler(time.time, time.sleep)

    def majority(self, votes):
        """ Return `True` if `votes` is a majority """
        return (len(votes) / len(self)) > self.majority_threshold

    def election_timeout(self):
        """ Return election timeout in seconds """
        return 0.15

    def heartbeat_interval(self):
        """ Return heartbeat interval (timeout) in seconds """
        return self.election_timeout() / 3.0

    def __len__(self):
        """ Size of the network (cluster) """
        return 1

    def send(self, rpc, dst=ALL):
        """ Send the rpc """
        raise NotImplementedError()
