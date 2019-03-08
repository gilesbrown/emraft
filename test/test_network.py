import pytest
from emraft.network import Network


#
# Our aim is to get most test coverage using tests in test_server.py
# and the tests are for things that cannot be sensibly tested that way.

def test_default_len():
    assert len(Network()) == 1


def test_send_raises_not_implemented():
    with pytest.raises(NotImplementedError):
        Network().send(None)
