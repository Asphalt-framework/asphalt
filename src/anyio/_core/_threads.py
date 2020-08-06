__all__ = ('run_sync_in_worker_thread', 'run_async_from_thread',
           'current_default_worker_thread_limiter', 'create_blocking_portal',
           'start_blocking_portal')

import threading
from typing import TypeVar, Callable, Optional, Coroutine, Any, Dict, Awaitable, cast

from ._eventloop import get_asynclib, threadlocals, run
from ..abc import CapacityLimiter, BlockingPortal

T_Retval = TypeVar('T_Retval', covariant=True)


def run_sync_in_worker_thread(func: Callable[..., T_Retval], *args, cancellable: bool = False,
                              limiter: Optional[CapacityLimiter] = None) -> Awaitable[T_Retval]:
    """
    Start a thread that calls the given function with the given arguments.

    If the ``cancellable`` option is enabled and the task waiting for its completion is cancelled,
    the thread will still run its course but its return value (or any raised exception) will be
    ignored.

    :param func: a callable
    :param args: positional arguments for the callable
    :param cancellable: ``True`` to allow cancellation of the operation
    :param limiter: capacity limiter to use to limit the total amount of threads running
        (if omitted, the default limiter is used)
    :return: an awaitable that yields the return value of the function.

    """
    return get_asynclib().run_sync_in_worker_thread(func, *args, cancellable=cancellable,
                                                    limiter=limiter)


def run_async_from_thread(func: Callable[..., Coroutine[Any, Any, T_Retval]], *args) -> T_Retval:
    """
    Call a coroutine function from a worker thread.

    :param func: a coroutine function
    :param args: positional arguments for the callable
    :return: the return value of the coroutine function

    """
    try:
        asynclib = threadlocals.current_async_module
    except AttributeError:
        raise RuntimeError('This function can only be run from an AnyIO worker thread')

    return asynclib.run_async_from_thread(func, *args)


def current_default_worker_thread_limiter() -> CapacityLimiter:
    """
    Return the capacity limiter that is used by default to limit the number of concurrent threads.

    :return: a capacity limiter object

    """
    return get_asynclib().current_default_thread_limiter()


def create_blocking_portal() -> BlockingPortal:
    """Create a portal for running functions in the event loop thread."""
    return get_asynclib().BlockingPortal()


def start_blocking_portal(
        backend: str = 'asyncio',
        backend_options: Optional[Dict[str, Any]] = None) -> BlockingPortal:
    """
    Start a new event loop in a new thread and run a blocking portal in its main task.

    :param backend:
    :param backend_options:
    :return: a blocking portal object

    """
    async def run_portal():
        nonlocal portal
        async with create_blocking_portal() as portal:
            event.set()
            await portal.sleep_until_stopped()

    portal: Optional[BlockingPortal]
    event = threading.Event()
    kwargs = {'func': run_portal, 'backend': backend, 'backend_options': backend_options}
    thread = threading.Thread(target=run, kwargs=kwargs)
    thread.start()
    event.wait()
    return cast(BlockingPortal, portal)
