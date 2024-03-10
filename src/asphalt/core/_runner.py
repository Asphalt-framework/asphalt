from __future__ import annotations

import platform
import signal
import sys
from contextlib import AsyncExitStack
from functools import partial
from logging import INFO, basicConfig, getLogger
from logging.config import dictConfig
from typing import Any, cast

import anyio
from anyio import (
    Event,
    fail_after,
    get_cancelled_exc_class,
    to_thread,
)
from anyio.abc import TaskStatus
from exceptiongroup import catch

from ._component import Component, component_types
from ._context import Context, start_service_task
from ._exceptions import ApplicationExit

if sys.version_info < (3, 11):
    from exceptiongroup import BaseExceptionGroup


async def handle_signals(event: Event, *, task_status: TaskStatus[None]) -> None:
    logger = getLogger(__name__)
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        task_status.started()
        async for signum in signals:
            signal_name = signal.strsignal(signum) or ""
            logger.info(
                "Received signal (%s) – terminating application",
                signal_name.split(":", 1)[0],  # macOS has ": <signum>" after the name
            )
            event.set()


def handle_application_exit(excgrp: BaseExceptionGroup[ApplicationExit]) -> None:
    exc: BaseException = excgrp
    while isinstance(exc, BaseExceptionGroup):
        if len(exc.exceptions) > 1:
            raise RuntimeError("Multiple ApplicationExit exceptions were raised")

        exc = exc.exceptions[0]

    exit_exc = cast(ApplicationExit, exc)
    if exit_exc.code:
        raise SystemExit(exit_exc.code).with_traceback(exc.__traceback__) from None


async def run_application(
    component: Component | dict[str, Any],
    *,
    max_threads: int | None = None,
    logging: dict[str, Any] | int | None = INFO,
    start_timeout: int | float | None = 10,
) -> None:
    """
    Configure logging and start the given component.

    Assuming the root component was started successfully, the event loop will continue
    running until the process is terminated.

    Initializes the logging system first based on the value of ``logging``:
      * If the value is a dictionary, it is passed to :func:`logging.config.dictConfig`
        as argument.
      * If the value is an integer, it is passed to :func:`logging.basicConfig` as the
        logging level.
      * If the value is ``None``, logging setup is skipped entirely.

    By default, the logging system is initialized using :func:`~logging.basicConfig`
    using the ``INFO`` logging level.

    The default executor in the event loop is replaced with a new
    :class:`~concurrent.futures.ThreadPoolExecutor` where the maximum number of threads
    is set to the value of ``max_threads`` or, if omitted, the default value of
    :class:`~concurrent.futures.ThreadPoolExecutor`.

    :param component: the root component (either a component instance or a configuration
        dictionary where the special ``type`` key is a component class
    :param max_threads: the maximum number of worker threads in the default thread pool
        executor (the default value depends on the event loop implementation)
    :param logging: a logging configuration dictionary, :ref:`logging level
        <python:levels>` or`None``
    :param start_timeout: seconds to wait for the root component (and its subcomponents)
        to start up before giving up (``None`` = wait forever)

    """
    # Configure the logging system
    if isinstance(logging, dict):
        dictConfig(logging)
    elif isinstance(logging, int):
        basicConfig(level=logging)

    # Inform the user whether -O or PYTHONOPTIMIZE was set when Python was launched
    logger = getLogger(__name__)
    logger.info("Running in %s mode", "development" if __debug__ else "production")

    # Apply the maximum worker thread limit
    if max_threads is not None:
        to_thread.current_default_thread_limiter().total_tokens = max_threads

    # Instantiate the root component if a dict was given
    if isinstance(component, dict):
        component = cast(Component, component_types.create_object(**component))

    logger.info("Starting application")
    try:
        async with AsyncExitStack() as exit_stack:
            handlers = {ApplicationExit: handle_application_exit}
            exit_stack.enter_context(catch(handlers))  # type: ignore[arg-type]
            event = Event()

            await exit_stack.enter_async_context(Context())
            if platform.system() != "Windows":
                await start_service_task(
                    partial(handle_signals, event), "Asphalt signal handler"
                )

            try:
                with fail_after(start_timeout):
                    await component.start()
            except TimeoutError:
                logger.error("Timeout waiting for the root component to start")
                raise
            except (ApplicationExit, get_cancelled_exc_class()):
                logger.error("Application startup interrupted")
                return
            except BaseException:
                logger.exception("Error during application startup")
                raise

            logger.info("Application started")

            await event.wait()
    finally:
        logger.info("Application stopped")
