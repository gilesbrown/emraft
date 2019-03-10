from emraft.network import Network
from emraft.rpc import RequestVoteResponse
from emraft.server import Server


class StopServer(Exception):
    """ Raised when we want to stop the Server """


class SimulatedNetwork(Network):

    def stop_server(self, server):
        raise StopServer()

    def __init__(self, servers):
        super(SimulatedNetwork, self).__init__()
        assert servers  # must have at least 1
        self.servers = servers
        # *this* server is the last in the list
        self.id = self.servers[-1]

        self.server = None
        self.sends = []

    def __len__(self):
        return len(self.servers)

    def request_vote(self, request, server, grant_vote):
        resp = RequestVoteResponse(term=self.server.current_term,
                                   sender=server,
                                   vote_granted=grant_vote)
        self.server.after(0, self.receive, rpc=resp)

    def send(self, rpc, dst=Network.ALL):
        self.sends.append((rpc, dst))
        attrname = 'send_{}'.format(rpc.__class__.__name__)
        method = getattr(self, attrname)
        response = method(rpc, dst)
        if response is not None:
            self.receive(response)


class VotingNetwork(SimulatedNetwork):

    def __init__(self, servers, grant_vote):
        super(VotingNetwork, self).__init__(servers)
        self.grant_vote = grant_vote

    def send_RequestVote(self, rpc, dst=Network.ALL):
        for server in self.servers[:-1]:
            if server == self.id:
                continue
            response = RequestVoteResponse(term=self.server.current_term,
                                           sender=server,
                                           vote_granted=self.grant_vote)
            self.server.receive(rpc=response)

# 
# class DontGrantVotesNetwork(SimulatedNetwork):
#     """ Simulate votes not being granted """
# 
#     def on_request_vote(self):
#         pass
# 
#     def send(self, rpc, dst=Network.ALL):
#         self.sends.append((rpc, dst))
#         if isinstance(rpc, RequestVote):
#             for server in self.servers[:-1]:
#                 self.request_vote(rpc, server, False)
#             self.on_request_vote()
#         elif isinstance(rpc, AppendEntriesResponse):
#             # we will send one of these wne we receive an AppendEntries
#             pass
#         else:
#             assert False, "unexpected rpc {!r}".format(rpc)
# 
# 
# class ReceiveAppendEntriesNetwork(DontGrantVotesNetwork):
# 
#     def on_request_vote(self):
#         append_entries = AppendEntries(term=self.server.current_term,
#                                        leader_id=self.servers[-2],
#                                        prev_log=(0, 0),
#                                        entries=[],
#                                        leader_commit=0)
#         self.server.receive(append_entries)
#         self.server.after(0.1, stop_server)
# 

def run_server(persistent_state, network, timeout=10.0, init_server=None):
    # persistent_state = MemPersistentState()
    network.server = Server(persistent_state, network)
    # add an overall timeout for the run
    network.server.after(timeout, network.stop_server)
    if init_server:
        init_server(network.server)
    try:
        network.server.scheduler.run()
    except StopServer:
        pass
    return network.server
