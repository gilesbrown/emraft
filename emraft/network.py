import time


class Network(object):

    ALL = object()

    timefunc = time.time
    delayfunc = time.sleep

    def __init__(self, **kw):
        self.id = id(self)
        self.server = None
        self.majority_threshold = kw.pop('majority_threshold', 0.5)

    def bind(self, server):
        self.server = server

    def majority(self, votes):
        return (len(votes) / len(self)) >= self.majority_threshold

    def __len__(self):
        return 1

    def election_timeout(self):
        return 0.15  # 150ms
        # return 2.0  # 150ms

    def heartbeat_interval(self):
        return self.election_timeout() / 3.0

    def send(self, rpc, dst=ALL):
        """ Send the rpc """
        raise NotImplementedError()
