import pytest
from emraft.rpc import RPC, log_key


def test_rpc_call():
    rpc = RPC(term=1, sender=1)
    with pytest.raises(NotImplementedError):
        rpc(None)


def test_log_key_wrong_length():
    with pytest.raises(ValueError):
        log_key([1, 2, 3])
