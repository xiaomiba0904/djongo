import time
from contextlib import ContextDecorator

from django.db import DEFAULT_DB_ALIAS, connections
from pymongo.errors import OperationFailure


def get_connection(using=None):
    """
    Get a database connection by name, or the default database connection
    if no name is provided. This is a private API.
    """
    if using is None:
        using = DEFAULT_DB_ALIAS
    return connections[using]


#################################
# Decorators / context managers #
#################################

class Atomic(ContextDecorator):
    """
    Guarantee the atomic execution of a given block.

    An instance can be used either as a decorator or as a context manager.

    When it's used as a decorator, __call__ wraps the execution of the
    decorated function in the instance itself, used as a context manager.

    When it's used as a context manager, __enter__ creates a transaction or a
    savepoint, depending on whether a transaction is already in progress, and
    __exit__ commits the transaction or releases the savepoint on normal exit,
    and rolls back the transaction or to the savepoint on exceptions.

    It's possible to disable the creation of savepoints if the goal is to
    ensure that some code runs within a transaction without creating overhead.

    A stack of savepoints identifiers is maintained as an attribute of the
    connection. None denotes the absence of a savepoint.

    This allows reentrancy even if the same AtomicWrapper is reused. For
    example, it's possible to define `oa = atomic('other')` and use `@oa` or
    `with oa:` multiple times.

    Since database connections are thread-local, this is thread-safe.

    An atomic block can be tagged as durable. In this case, raise a
    RuntimeError if it's nested within another atomic block. This guarantees
    that database changes in a durable block are committed to the database when
    the block exists without error.

    This is a private API.
    """
    # This private flag is provided only to disable the durability checks in
    # TestCase.
    _ensure_durability = True

    def __init__(self, using, savepoint, durable, client=None):
        self.using = using
        self.savepoint = savepoint
        self.durable = durable
        self.session = None
        self.client = client

    def __enter__(self):
        self.start()
        return self

    def get_client(self):
        if self.client is not None:
            return self.client

        db = get_connection(self.using)
        if db.client_connection is None:
            client = db.get_new_connection(db.get_connection_params()).client
        else:
            client = db.client_connection.client
        return client

    def start(self):
        self.client = self.get_client()
        self.session = self.client.start_session()
        setattr(self.client, 'session', self.session)

        self.session.start_transaction()

    def commit(self):
        if self.session.in_transaction:
            self.commit_with_retry(self.session)
        self.session.end_session()
        self.clear()

    def clear(self):
        if self.client is not None:
            delattr(self.client, 'session')

    @staticmethod
    def commit_with_retry(session, retry_count=1000, timeout=30):
        count = 0
        start_time = time.time()

        while True:
            try:
                session.commit_transaction()
                break
            except OperationFailure as e:
                count += 1
                if count > retry_count:
                    raise e
                if time.time() - start_time >= timeout:
                    raise e

    def abort(self):
        if self.session.in_transaction:
            self.session.abort_transaction()
        self.session.end_session()
        self.clear()

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            return self.commit()
        else:
            return self.abort()


def atomic(using=None, savepoint=True, durable=False):
    # Bare decorator: @atomic -- although the first argument is called
    # `using`, it's actually the function being decorated.
    if callable(using):
        return Atomic(DEFAULT_DB_ALIAS, savepoint, durable)(using)
    # Decorator: @atomic(...) or context manager: with atomic(...): ...
    else:
        return Atomic(using, savepoint, durable)
