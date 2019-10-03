from hypothesis import given
from hypothesis import strategies as st

from fennel.utils import backoff, base64uuid


@given(st.integers(min_value=0, max_value=1024))
def test_backoff(n):
    assert 1 <= backoff(n) <= 604800


def test_uuid():
    xs = [base64uuid() for _ in range(100)]
    assert all(len(x) == 22 for x in xs)
