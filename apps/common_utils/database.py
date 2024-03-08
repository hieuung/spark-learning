import json
from contextlib import contextmanager
from functools import wraps

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from common_utils.cache import cache
from common_utils.exception import APIMessageError


@cache('env_config')
def _get_env_config():
    if 'ENV_CONFIG' not in os.environ:
        raise Exception('Could not find ENV_CONFIG. Ensure "with_env_config: true" is provided in your lambda config')

    return json.loads(os.environ['ENV_CONFIG'])


def env_config_fields(*fields):
    def env_config_wrapper(func):
        @wraps(func)
        def _execute(*args, **kwargs):
            config = _get_env_config()

            for field in fields:
                kwargs[field] = config[field]

            return func(*args, **kwargs)

        return _execute

    return env_config_wrapper


@cache('sql_session')
@env_config_fields('SQL_URI')
def _get_session_class(SQL_URI):
    engine = create_engine(SQL_URI)
    return sessionmaker(
        bind=engine,
        # Disable this allow to access to object after session is close
        expire_on_commit=False
    )


@contextmanager
def session():
    """Provide a transactional scope around a series of operations."""
    session_class = _get_session_class()
    _session = session_class()
    try:
        yield _session
    finally:
        _session.close()


def with_session(func):
    @wraps(func)
    def with_session_func_wrapper(*args, **kwargs):
        if 'session' in kwargs:
            return func(*args, **kwargs)

        with session() as sess:
            return func(*args, session=sess, **kwargs)

    return with_session_func_wrapper


@contextmanager
def transaction():
    """Provide a transactional scope around a series of operations."""
    session_class = _get_session_class()
    _session = session_class()
    try:
        yield _session
        _session.commit()
    except:
        _session.rollback()
        raise
    finally:
        _session.close()


def with_transaction(func):
    @wraps(func)
    def transaction_func_wrapper(*args, **kwargs):
        if 'session' in kwargs:
            return func(*args, **kwargs)

        with transaction() as sess:
            return func(*args, session=sess, **kwargs)

    return transaction_func_wrapper


def require(*fields):
    def wrapper(func):
        @wraps(func)
        def wrapper2(obj, *args, **kwargs):
            for field in fields:
                if getattr(obj, field) is None:
                    raise APIMessageError(
                        "SystemError: Field '%s' is not provided in %s" % (
                            field, obj.__class__.__name__))

            return func(obj, *args, **kwargs)

        return wrapper2

    return wrapper