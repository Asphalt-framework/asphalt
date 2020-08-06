from abc import abstractmethod
from typing import Generic, TypeVar, Union, Callable, Any, Optional

from .resources import AsyncResource
from .tasks import TaskGroup
from ..exceptions import EndOfStream

T_Item = TypeVar('T_Item')
T_Stream = TypeVar('T_Stream')


class UnreliableObjectReceiveStream(Generic[T_Item], AsyncResource):
    """
    An interface for receiving objects.

    This interface makes no guarantees that the received messages arrive in the order in which they
    were sent, or that no messages are missed.

    Asynchronously iterating over objects of this type will yield objects matching the given type
    parameter.
    """

    def __aiter__(self):
        return self

    async def __anext__(self) -> T_Item:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration

    @abstractmethod
    async def receive(self) -> T_Item:
        """
        Receive the next item.

        :raises anyio.exceptions.ClosedResourceError: if the receive stream has been explicitly
            closed
        :raises EndOfStream: if this stream has been closed from the other end
        :raises BrokenResourceError: if this stream has been rendered unusable due to external
            causes
        """


class UnreliableObjectSendStream(Generic[T_Item], AsyncResource):
    """
    An interface for sending objects.

    This interface makes no guarantees that the messages sent will reach the recipient(s) in the
    same order in which they were sent, or at all.
    """

    @abstractmethod
    async def send(self, item: T_Item) -> None:
        """
        Send an item to the peer(s).

        :param item: the item to send
        :raises anyio.exceptions.ClosedResourceError: if the send stream has been explicitly closed
        :raises BrokenResourceError: if this stream has been rendered unusable due to external
            causes
        """


class UnreliableObjectStream(Generic[T_Item], UnreliableObjectReceiveStream[T_Item],
                             UnreliableObjectSendStream[T_Item]):
    """
    A bidirectional message stream which does not guarantee the order or reliability of message
    delivery.
    """


class ObjectReceiveStream(Generic[T_Item], UnreliableObjectReceiveStream[T_Item]):
    """
    A receive message stream which guarantees that messages are received in the same order in
    which they were sent, and that no messages are missed.
    """


class ObjectSendStream(Generic[T_Item], UnreliableObjectSendStream[T_Item]):
    """
    A send message stream which guarantees that messages are delivered in the same order in which
    they were sent, without missing any messages in the middle.
    """


class ObjectStream(Generic[T_Item], ObjectReceiveStream[T_Item], ObjectSendStream[T_Item],
                   UnreliableObjectStream[T_Item]):
    """
    A bidirectional message stream which guarantees the order and reliability of message delivery.
    """

    @abstractmethod
    async def send_eof(self) -> None:
        """
        Send an end-of-file indication to the peer.

        You should not try to send any further data to this stream after calling this method.
        This method is idempotent (does nothing on successive calls).
        """


class ByteReceiveStream(AsyncResource):
    """
    An interface for receiving bytes from a single peer.

    Iterating this byte stream will yield a byte string of arbitrary length, but no more than
    65536 bytes.
    """

    def __aiter__(self) -> 'ByteReceiveStream':
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive()
        except EndOfStream:
            raise StopAsyncIteration

    @abstractmethod
    async def receive(self, max_bytes: int = 65536) -> bytes:
        """
        Receive at most ``max_bytes`` bytes from the peer.

        :param max_bytes: maximum number of bytes to receive
        :return: the received bytes
        :raises EndOfStream: if this stream has been closed from the other end
        """


class ByteSendStream(AsyncResource):
    """An interface for sending bytes to a single peer."""

    @abstractmethod
    async def send(self, item: bytes) -> None:
        """
        Send the given bytes to the peer.

        :param item: the bytes to send
        """


class ByteStream(ByteReceiveStream, ByteSendStream):
    """A bidirectional byte stream."""

    @abstractmethod
    async def send_eof(self) -> None:
        """
        Send an end-of-file indication to the peer.

        You should not try to send any further data to this stream after calling this method.
        This method is idempotent (does nothing on successive calls).
        """


#: type alias for all unreliable bytes-oriented receive streams
AnyUnreliableByteReceiveStream = Union[UnreliableObjectReceiveStream[bytes], ByteReceiveStream]
#: type alias for all unreliable bytes-oriented send streams
AnyUnreliableByteSendStream = Union[UnreliableObjectSendStream[bytes], ByteSendStream]
#: type alias for all unreliable bytes-oriented streams
AnyUnreliableByteStream = Union[UnreliableObjectStream[bytes], ByteStream]
#: type alias for all bytes-oriented receive streams
AnyByteReceiveStream = Union[ObjectReceiveStream[bytes], ByteReceiveStream]
#: type alias for all bytes-oriented send streams
AnyByteSendStream = Union[ObjectSendStream[bytes], ByteSendStream]
#: type alias for all bytes-oriented streams
AnyByteStream = Union[ObjectStream[bytes], ByteStream]


class Listener(Generic[T_Stream], AsyncResource):
    """An interface for objects that let you accept incoming connections."""

    @abstractmethod
    async def serve(self, handler: Callable[[T_Stream], Any],
                    task_group: Optional[TaskGroup] = None) -> None:
        """
        Accept incoming connections as they come in and spawn tasks to handle them.

        :param handler: a callable that will be used to handle each accepted connection
        :param task_group: the task group that will be used to spawn tasks for handling each
            accepted connection (if omitted, an ad-hoc task group will be created)
        """
