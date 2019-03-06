from .rpc import AppendEntries


class Leader:
    """ Leader state """

    def __init__(self, server):
        self.next_index = {}
        self.match_index = {}
        self.heartbeat(server)

    def heartbeat(self, server):
        last_log_index, last_log_term = server.log.last()
        heartbeat = AppendEntries(
            term=server.current_term,
            leader_id=server.network.id,
            prev_log_index=last_log_index,
            prev_log_term=last_log_term,
            entries=[],
            leader_commit=server.commit_index
        )
        server.network.send(heartbeat)
        delay = server.network.heartbeat_interval()
        server.after(delay, self.heartbeat)
