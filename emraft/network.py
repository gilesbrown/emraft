import time


class Network(object):

    ALL = object()

    timefunc = time.time
    delayfunc = time.sleep

    def __init__(self):
        self.id = id(self)
        self.server = None

    def majority(self, votes):
        return (len(votes) / len(self)) >= 0.5

    def bind(self, server):
        self.server = server

    def __len__(self):
        return 1

    def election_timeout(self):
        return 0.15  # 150ms
        # return 2.0  # 150ms

    def send(self, rpc, dst=ALL):
        """ Send the rpc """
        raise NotImplementedError()
