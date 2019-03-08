class MemLog:
    """ In-memory Raft log (for testing) """

    def __init__(self):
        self.log = [(0, None)]

    def get_term(self, index):
        if index < len(self.log):
            return self.log[index][0]

    def append(self, entries):
        pass

    def last(self):
        log_index = len(self.log) - 1
        return (self.log[log_index][0], log_index)


class MemPersistentState:
    """ In-memory "persistent" server state (for testing) """

    def __init__(self, current_term=0, voted_for=None):
        self._current_term = current_term
        self.voted_for = voted_for
        self.log = MemLog()

    def set_current_term(self, term):
        if term < self._current_term:
            raise ValueError()
        if term > self._current_term:
            self._voted_for = None
        self._current_term = term

    def get_current_term(self):
        return self._current_term

    def set_voted_for(self, voted_for):
        self._voted_for = voted_for

    def get_voted_for(self):
        return self._voted_for
