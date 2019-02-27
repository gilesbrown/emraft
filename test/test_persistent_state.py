import pytest
from emraft.persistent_state import SQLitePersistentState


def test_current_term(tmpdir):
    persistent_state = SQLitePersistentState.connect(':memory:')
    assert persistent_state.get_current_term() == 0
    persistent_state.set_current_term(1)
    assert persistent_state.get_current_term() == 1


def test_current_term_monotonic_increase(tmpdir):
    persistent_state = SQLitePersistentState.connect(':memory:')
    assert persistent_state.get_current_term() == 0
    persistent_state.set_current_term(2)
    assert persistent_state.get_current_term() == 2
    with pytest.raises(ValueError):
        persistent_state.set_current_term(1)
