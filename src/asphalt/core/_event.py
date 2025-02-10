from __future__ import annotations

import weakref
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Callable,
    Generator,
    Hashable,
    Sequence,
)
from contextlib import (
    AbstractAsyncContextManager,
    AsyncExitStack,
    asynccontextmanager,
    contextmanager,
)
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import time as stdlib_time
from typing import Any, Generic, TypeVar
from warnings import warn
from weakref import ReferenceType, WeakKeyDictionary

from anyio import BrokenResourceError, WouldBlock, create_memory_object_stream
from anyio.streams.memory import MemoryObjectSendStream

from ._exceptions import UnboundSignal
from ._utils import qualified_name

T_Event = TypeVar("T_Event", bound="Event")
bound_signals = WeakKeyDictionary[Hashable, "Signal[Any]"]()


class SignalQueueFull(UserWarning):
    """
    Warning about signal delivery failing due to a subscriber's queue being full
    because the subscriber could not receive the events quickly enough.
    """


class Event:
    """
    The base class for all events.

    :ivar source: the object where this event originated from
    :ivar str topic: the topic
    :ivar float time: event creation time as seconds from the UNIX epoch
    """

    __slots__ = "source", "time", "topic"

    source: Any
    topic: str
    time: float

    @property
    def utc_timestamp(self) -> datetime:
        """
        Return a timezone aware :class:`~datetime.datetime` corresponding to the
        ``time`` variable, using the UTC timezone.

        """
        return datetime.fromtimestamp(self.time, timezone.utc)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(source={self.source!r}, topic={self.topic!r})"
        )


@dataclass
class Signal(Generic[T_Event]):
    """
    Declaration of a signal that can be used to dispatch events.

    This is a descriptor that returns itself on class level attribute access and a bound
    version of itself on instance level access. Dispatching and streaming
    events only works with these bound instances.

    Each signal must be assigned to a class attribute, but only once. The Signal will
    not function correctly if the same Signal instance is assigned to multiple
    attributes.

    :param event_class: an event class
    """

    event_class: type[T_Event]

    _instance: ReferenceType[Hashable] = field(init=False)
    _topic: str = field(init=False)
    _send_streams: list[MemoryObjectSendStream[T_Event]] = field(init=False)

    def __get__(self, instance: Hashable, owner: Any) -> Signal[T_Event]:
        if instance is None:
            return self

        try:
            return bound_signals[instance]
        except KeyError:
            bound_signal = Signal(self.event_class)
            bound_signal._topic = self._topic
            bound_signal._instance = weakref.ref(instance)
            bound_signal._send_streams = []
            bound_signals[instance] = bound_signal
            return bound_signal

    def __set_name__(self, owner: Any, name: str) -> None:
        self._topic = name

    def _check_is_bound_signal(self) -> None:
        if not hasattr(self, "_instance"):
            raise UnboundSignal

    @contextmanager
    def _subscribe(self, send: MemoryObjectSendStream[T_Event]) -> Generator[None]:
        self._check_is_bound_signal()
        self._send_streams.append(send)
        try:
            yield
        finally:
            self._send_streams.remove(send)

    def dispatch(self, event: T_Event) -> None:
        """
        Dispatch an event.

        :raises UnboundSignal: if attempting to dispatch an event on a signal not bound
            to any instance of the containing class

        """
        self._check_is_bound_signal()
        if not isinstance(event, self.event_class):
            raise TypeError(
                f"Event type mismatch: event ({qualified_name(event)}) is not a "
                f"subclass of {qualified_name(self.event_class)}"
            )

        event.source = self._instance()
        event.topic = self._topic
        event.time = stdlib_time()

        for stream in list(self._send_streams):
            try:
                stream.send_nowait(event)
            except BrokenResourceError:
                pass
            except WouldBlock:
                warn(
                    f"Queue full ({stream.statistics().max_buffer_size}) when trying "
                    f"to send dispatched event to subscriber",
                    SignalQueueFull,
                    stacklevel=2,
                )

    async def wait_event(
        self,
        filter: Callable[[T_Event], bool] | None = None,
    ) -> T_Event:
        """
        Shortcut for calling :func:`wait_event` with this signal in the first argument.

        """
        return await wait_event([self], filter)

    def stream_events(
        self,
        filter: Callable[[T_Event], bool] | None = None,
        *,
        max_queue_size: int = 50,
    ) -> AbstractAsyncContextManager[AsyncIterator[T_Event]]:
        """
        Shortcut for calling :func:`stream_events` with this signal in the first
        argument.

        """
        return stream_events([self], filter, max_queue_size=max_queue_size)


@asynccontextmanager
async def stream_events(
    signals: Sequence[Signal[T_Event]],
    filter: Callable[[T_Event], bool] | None = None,
    *,
    max_queue_size: int = 50,
) -> AsyncIterator[AsyncIterator[T_Event]]:
    """
    Return an async generator that yields events from the given signals.

    Only events that pass the filter callable (if one has been given) are returned.
    If no filter function was given, all events are yielded from the generator.

    If another event is received from any of the signals while the previous one is still
    being yielded, it will stay in the queue. If the queue fills up, then
    :meth:`~.Signal.dispatch` on all the signals will block until the yield has been
    processed.

    The listening of events from the given signals starts when this function is called.

    :param signals: the signals to get events from
    :param filter: a callable that takes an event object as an argument and returns a
        truthy value if the event should pass
    :param max_queue_size: maximum number of unprocessed events in the queue
    :return: an async generator yielding all events (that pass the filter, if any) from
        all the given signals
    :raises UnboundSignal: if attempting to listen to events on a signal not bound to
        any instance of the containing class

    """

    async def filter_events() -> AsyncGenerator[T_Event, None]:
        async for event in receive:
            if filter is None or filter(event):
                yield event

    send, receive = create_memory_object_stream[T_Event](max_queue_size)
    async with AsyncExitStack() as exit_stack:
        filtered_receive = filter_events()
        exit_stack.push_async_callback(filtered_receive.aclose)
        exit_stack.enter_context(send)
        exit_stack.enter_context(receive)
        for signal in signals:
            exit_stack.enter_context(signal._subscribe(send))

        yield filtered_receive


async def wait_event(
    signals: Sequence[Signal[T_Event]],
    filter: Callable[[T_Event], bool] | None = None,
) -> T_Event:
    """
    Wait until any of the given signals dispatches an event that satisfies the filter
    (if any).

    If no filter has been given, the first event dispatched from any of the signals is
    returned.

    The listening of events from the given signals starts when this function is called.

    :param signals: the signals to get events from
    :param filter: a callable that takes an event object as an argument and returns a
        truthy value if the event should pass
    :return: the first event (that passed the filter, if any) that was dispatched from
        any of the signals
    :raises UnboundSignal: if attempting to listen to events on a signal not bound to
        any instance of the containing class

    """
    async with stream_events(signals, filter) as stream:
        return await stream.__anext__()
