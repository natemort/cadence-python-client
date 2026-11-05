"""An in-process ``grpc.aio.Channel`` that invokes servicers directly.

Python's grpcio doesn't expose an in-process transport and ``grpc_testing`` has
no asyncio support. Requests and responses are still serialized on both
sides, so the client and server never share mutable messages.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable, Generator, Sequence
from typing import Any, NoReturn, cast

import grpc
from grpc.aio import (
    AioRpcError,
    Channel,
    ClientCallDetails,
    ClientInterceptor,
    Metadata,
    UnaryUnaryCall,
    UnaryUnaryClientInterceptor,
)

from cadence.testing._server._errors import ServiceError

logger = logging.getLogger(__name__)


def _identity(value: Any) -> Any:
    return value


def _remaining(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - time.monotonic())


def _deadline(timeout: float | None) -> float | None:
    return None if timeout is None else time.monotonic() + timeout


def _rpc_error(
    code: grpc.StatusCode, details: str, trailing_metadata: Metadata | None = None
) -> AioRpcError:
    return AioRpcError(code, Metadata(), trailing_metadata or Metadata(), details)


class _ServicerContext:
    """The parts of ``grpc.aio.ServicerContext`` the servicers use."""

    def __init__(self, deadline: float | None) -> None:
        self._deadline = deadline
        self.code: grpc.StatusCode | None = None
        self.details = ""

    def time_remaining(self) -> float | None:
        return _remaining(self._deadline)

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


class InMemoryServer:
    """Holds servicer handlers and creates channels bound to them.

    Generated ``add_*Servicer_to_server`` functions accept it in place of a
    ``grpc.aio.Server``.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, grpc.RpcMethodHandler] = {}

    def add_generic_rpc_handlers(self, handlers: Any) -> None:
        # The same methods also arrive through add_registered_method_handlers.
        pass

    def add_registered_method_handlers(
        self, service_name: str, method_handlers: dict[str, grpc.RpcMethodHandler]
    ) -> None:
        for name, handler in method_handlers.items():
            self._handlers[f"/{service_name}/{name}"] = handler

    async def invoke(
        self, method: str, request: bytes, deadline: float | None
    ) -> bytes:
        """Run one unary call, raising its failure as an ``AioRpcError``."""
        handler = self._handlers.get(method)
        if handler is None or handler.unary_unary is None:
            raise _rpc_error(
                grpc.StatusCode.UNIMPLEMENTED, f"Method not found: {method}"
            )
        deserialize = handler.request_deserializer or _identity
        # Duck-typed: implements the ServicerContext methods servicers call.
        context: Any = _ServicerContext(deadline)
        try:
            async with asyncio.timeout(_remaining(deadline)):
                response = await handler.unary_unary(deserialize(request), context)
        except ServiceError as e:
            raise _rpc_error(e.code, e.message, e.trailing_metadata()) from None
        except TimeoutError:
            raise _rpc_error(
                grpc.StatusCode.DEADLINE_EXCEEDED, "Deadline Exceeded"
            ) from None
        except Exception as e:
            # The generated base servicer sets UNIMPLEMENTED, then raises.
            if context.code is not None:
                raise _rpc_error(context.code, context.details) from None
            logger.exception("In-memory servicer %s failed", method)
            raise _rpc_error(
                grpc.StatusCode.UNKNOWN, f"Unexpected {type(e).__name__}: {e}"
            ) from None
        serialize = handler.response_serializer or _identity
        result: bytes = serialize(response)
        return result

    def channel(self, interceptors: Sequence[ClientInterceptor] = ()) -> Channel:
        return InMemoryChannel(self, interceptors)


class _Call(UnaryUnaryCall):
    """A call running as a task on the current loop.

    As in grpc.aio, an intercepted call takes its status from ``result``, the
    call its interceptor chain returned. Otherwise the status comes from the
    ``AioRpcError`` the task failed with.
    """

    def __init__(self, coro: Awaitable[Any], deadline: float | None) -> None:
        self._task = asyncio.ensure_future(coro)
        self._deadline = deadline
        self.result: UnaryUnaryCall | None = None

    async def _wait(self) -> UnaryUnaryCall | None:
        await asyncio.wait({self._task})
        return self.result

    def _error(self) -> AioRpcError | None:
        if self._task.cancelled():
            return _rpc_error(grpc.StatusCode.CANCELLED, "Locally cancelled")
        error = self._task.exception()
        if error is None or isinstance(error, AioRpcError):
            return error
        return _rpc_error(grpc.StatusCode.UNKNOWN, str(error))

    def __await__(self) -> Generator[Any, None, Any]:
        return self._task.__await__()

    async def initial_metadata(self) -> Metadata:
        return Metadata()

    async def trailing_metadata(self) -> Metadata:
        if (result := await self._wait()) is not None:
            return await result.trailing_metadata()
        error = self._error()
        # Our errors always carry Metadata.
        return cast(Metadata, error.trailing_metadata()) if error else Metadata()

    async def code(self) -> grpc.StatusCode:
        if (result := await self._wait()) is not None:
            return await result.code()
        error = self._error()
        return grpc.StatusCode.OK if error is None else error.code()

    async def details(self) -> str:
        if (result := await self._wait()) is not None:
            return await result.details()
        error = self._error()
        return "" if error is None else error.details() or ""

    async def wait_for_connection(self) -> None:
        return None

    def cancelled(self) -> bool:
        return self._task.cancelled()

    def done(self) -> bool:
        return self._task.done()

    def time_remaining(self) -> float | None:
        return _remaining(self._deadline)

    def cancel(self) -> bool:
        # Also cancels the call the task is awaiting.
        return self._task.cancel()

    def add_done_callback(self, callback: Callable[[Any], None]) -> None:
        self._task.add_done_callback(lambda _: callback(self))


class InMemoryChannel(Channel):
    def __init__(
        self, server: InMemoryServer, interceptors: Sequence[ClientInterceptor]
    ) -> None:
        self._server = server
        self._interceptors = [
            i for i in interceptors if isinstance(i, UnaryUnaryClientInterceptor)
        ]
        self._tasks: set[asyncio.Future[Any]] = set()
        self._closed = False

    def _start(self, coro: Awaitable[Any], deadline: float | None) -> _Call:
        call = _Call(coro, deadline)
        self._tasks.add(call._task)
        call._task.add_done_callback(self._tasks.discard)
        return call

    def unary_unary(
        self,
        method: str,
        request_serializer: Callable[[Any], bytes] | None = None,
        response_deserializer: Callable[[bytes], Any] | None = None,
        _registered_method: bool | None = False,
    ) -> Any:
        serialize = request_serializer or _identity
        deserialize = response_deserializer or _identity

        async def invoke(request: Any, deadline: float | None) -> Any:
            response = await self._server.invoke(method, serialize(request), deadline)
            return deserialize(response)

        async def terminal(details: ClientCallDetails, request: Any) -> UnaryUnaryCall:
            deadline = _deadline(details.timeout)
            return self._start(invoke(request, deadline), deadline)

        continuation: Callable[[ClientCallDetails, Any], Awaitable[Any]] = terminal
        for interceptor in reversed(self._interceptors):
            continuation = functools.partial(
                interceptor.intercept_unary_unary, continuation
            )

        def call(
            request: Any,
            *,
            timeout: float | None = None,
            metadata: Any = None,
            credentials: Any = None,
            wait_for_ready: bool | None = None,
            compression: Any = None,
        ) -> UnaryUnaryCall:
            async def run() -> Any:
                result = await continuation(details, request)
                # Like grpc.aio, an interceptor may return the response itself.
                if not isinstance(result, UnaryUnaryCall):
                    return result
                outer.result = result
                return await result

            details = ClientCallDetails(
                method=method.encode(),  # type: ignore[arg-type]
                timeout=timeout,
                metadata=Metadata(*(metadata or ())),
                credentials=credentials,
                wait_for_ready=wait_for_ready,
            )
            outer = self._start(run(), _deadline(timeout))
            return outer

        return call

    def _unsupported(self, *args: Any, **kwargs: Any) -> NoReturn:
        raise NotImplementedError("streaming RPCs are not supported in memory")

    unary_stream = stream_unary = stream_stream = _unsupported

    async def close(self, grace: float | None = None) -> None:
        self._closed = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.wait(tasks)

    def get_state(self, try_to_connect: bool = False) -> grpc.ChannelConnectivity:
        if self._closed:
            return grpc.ChannelConnectivity.SHUTDOWN
        return grpc.ChannelConnectivity.READY

    async def wait_for_state_change(
        self, last_observed_state: grpc.ChannelConnectivity
    ) -> None:
        raise NotImplementedError("in-memory channels never change state")

    async def channel_ready(self) -> None:
        return None

    async def __aenter__(self) -> "InMemoryChannel":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()
