import os
import pytest
from pathlib import Path
from multiprocessing import Process

from app.models import Experiment, ResourceLock
from utils.common import DbEngine

ENV = os.environ["CS_ENV"] = "test"
CONFIG_PATH = Path(__file__) / ".." / ".." / "config" / ("%s.json" % ENV)
CONFIG_PATH = str(CONFIG_PATH.resolve())
LOCK_TIMEOUT = 2


def cleanup_session(session):
    session.query(Experiment).delete()
    session.query(ResourceLock).delete()
    session.commit()


def create_dummy_experiment(experiment_id, start_time=None):
    label = "dummy_%d" % experiment_id
    return Experiment(
        name = label,
        controller = label,
        start_time = start_time
    )


def validate_cooplock_state(session, cooplock, resource, experiment_id):
    lock_session, lock_rows = cooplock
    assert len(lock_rows) == 1
    assert lock_rows[0].resource == resource
    assert lock_rows[0].experiment_id == experiment_id


def validate_resource_is_locked(session, resource, experiment_id):
    # The worker process will attempt to acquire a lock that's already in use.
    # If the worker times out then it was unable to acquire the lock and the
    # test has passed. This is written this way since using a more straight
    # forward approach like calling with_for_update(nowait=True) and catching
    # the exception won't work since nowait is ignored in MySQL 5.7. This will
    # work fine so long as two different test cases don't attempt to use the
    # same resource name.
    def acquire_locked_resource():
        query = session.query(ResourceLock).with_for_update().filter_by(
            resource=resource,
            experiment_id=experiment_id
        ).all()
    worker = Process(target=acquire_locked_resource)
    worker.start()
    worker.join(LOCK_TIMEOUT)
    assert worker.is_alive()


@pytest.fixture(scope="module")
def session():
    session = DbEngine(CONFIG_PATH).new_session()
    cleanup_session(session)
    yield session
    cleanup_session(session)
    session.close()


def test_single_lock(session):
    resource = "test_single_lock"
    experiment_id = 1
    with session.cooplock(resource, experiment_id) as cooplock:
        validate_cooplock_state(session, cooplock, resource, experiment_id)
        validate_resource_is_locked(session, resource, experiment_id)


def test_single_lock_is_reusable(session):
    resource = "test_single_lock_is_reusable"
    experiment_id = 1
    with session.cooplock(resource, experiment_id) as cooplock:
        validate_cooplock_state(session, cooplock, resource, experiment_id)
        validate_resource_is_locked(session, resource, experiment_id)
    with session.cooplock(resource, experiment_id) as cooplock:
        validate_cooplock_state(session, cooplock, resource, experiment_id)
        validate_resource_is_locked(session, resource, experiment_id)


def test_lock_with_existing_transaction_in_progress(session):
    resource = "test_lock_with_existing_transaction_in_progress"
    experiment_id = 1
    session.add(create_dummy_experiment(experiment_id))
    session.flush()
    with session.cooplock(resource, experiment_id) as cooplock:
        validate_cooplock_state(session, cooplock, resource, experiment_id)
        validate_resource_is_locked(session, resource, experiment_id)
        assert len(session.query(Experiment).all()) == 1


def test_nested_locks(session):
    resource_1 = "test_nested_outer_lock"
    resource_2 = "test_nested_inner_lock"
    experiment_id = 1
    with session.cooplock(resource_1, experiment_id) as cooplock_1:
        with session.cooplock(resource_2, experiment_id) as cooplock_2:
            validate_cooplock_state(session, cooplock_1, resource_1, experiment_id)
            validate_resource_is_locked(session, resource_1, experiment_id)
            validate_cooplock_state(session, cooplock_2, resource_2, experiment_id)
            validate_resource_is_locked(session, resource_2, experiment_id)
        
