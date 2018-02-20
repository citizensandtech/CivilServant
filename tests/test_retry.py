import pytest

from utils.retry import retryable, RETRY_MAX_TIMES, RETRY_WAIT, BACKOFF_BASE, \
                        BACKOFF_MAX_EXP, BACKOFF_MAX_TIMES

ERRORING_FN_ERROR = NotImplementedError
WORKING_FN_RESULT = "Yay!"

def backoff_in_range(seconds, exp):
    return seconds >= BACKOFF_BASE \
       and seconds <= BACKOFF_BASE + BACKOFF_BASE**exp

def erroring_fn():
    raise ERRORING_FN_ERROR

def working_fn():
    return WORKING_FN_RESULT

def test_retryable_retry_and_backoff_disabled():
    test_fn = retryable(erroring_fn, retry=False, backoff=False, _testing=True)
    with pytest.raises(ERRORING_FN_ERROR) as e:
        test_fn()

    assert type(test_fn._retryable_last_exception) is ERRORING_FN_ERROR
    assert len(test_fn._retryable_sleep_periods) == 1
    assert all(secs == 0 for secs in test_fn._retryable_sleep_periods)

def test_retryable_retry_only():
    test_fn = retryable(erroring_fn, retry=True, backoff=False, _testing=True)
    with pytest.raises(ERRORING_FN_ERROR) as e:
        test_fn()

    assert type(test_fn._retryable_last_exception) is ERRORING_FN_ERROR
    assert len(test_fn._retryable_sleep_periods) == RETRY_MAX_TIMES
    assert all(secs == RETRY_WAIT for secs in test_fn._retryable_sleep_periods)

def test_retryable_backoff_only():
    test_fn = retryable(erroring_fn, retry=False, backoff=True, _testing=True)
    with pytest.raises(ERRORING_FN_ERROR) as e:
        test_fn()

    assert type(test_fn._retryable_last_exception) is ERRORING_FN_ERROR
    assert len(test_fn._retryable_sleep_periods) == BACKOFF_MAX_TIMES
    
    backoffs = zip(test_fn._retryable_sleep_periods, retryable._backoff_exps())
    assert all(backoff_in_range(secs, exp) for secs, exp in backoffs)

def test_retryable_retry_and_backoff_enabled():
    test_fn = retryable(erroring_fn, retry=True, backoff=True, _testing=True)
    with pytest.raises(ERRORING_FN_ERROR) as e:
        test_fn()

    assert type(test_fn._retryable_last_exception) is ERRORING_FN_ERROR
    
    max_times = RETRY_MAX_TIMES + BACKOFF_MAX_TIMES
    assert len(test_fn._retryable_sleep_periods) == max_times

    retry_periods = test_fn._retryable_sleep_periods[:RETRY_MAX_TIMES]
    assert all(secs == RETRY_WAIT for secs in retry_periods)

    backoff_periods = test_fn._retryable_sleep_periods[RETRY_MAX_TIMES:]
    backoffs = zip(backoff_periods, retryable._backoff_exps())
    assert all(backoff_in_range(secs, exp) for secs, exp in backoffs)

def test_retryable_non_erroring_fn():
    test_fn = retryable(working_fn, retry=True, backoff=True, _testing=True)
    assert test_fn() == WORKING_FN_RESULT

    assert test_fn._retryable_last_exception is None
    assert len(test_fn._retryable_sleep_periods) == 1
    assert all(secs == RETRY_WAIT for secs in test_fn._retryable_sleep_periods)

