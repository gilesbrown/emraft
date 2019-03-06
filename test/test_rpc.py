import pytest
from emraft.rpc import RPC


def test_rpc_call():
    rpc = RPC(term=1, sender=1)
    with pytest.raises(NotImplementedError):
        rpc(None)
