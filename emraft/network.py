import time
import sched


class Network(object):

    ALL = object()

    def __init__(self, **kw):
        self.id = id(self)
        self.receive = None
        self.majority_threshold = kw.pop('majority_threshold', 0.5)

    def scheduler(self):
        """ Return a scheduler """
        return sched.scheduler(time.time, time.sleep)

    def majority(self, votes):
        return (len(votes) / len(self)) > self.majority_threshold

    def __len__(self):
        """ Size of the network (cluster) """
        return 1

    def election_timeout(self):
        """ Return election timeout in seconds """
        return 0.15

    def heartbeat_interval(self):
        """ Return heartbeat interval (timeout) in seconds """
        return self.election_timeout() / 3.0

    def send(self, rpc, dst=ALL):
        """ Send the rpc """
        raise NotImplementedError()
