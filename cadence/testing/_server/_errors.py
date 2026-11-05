"""Service errors encoded the way the Cadence frontend encodes them.

Servicers raise ``ServiceError``, which ``InMemoryServer`` turns into a gRPC
error.
"""

from __future__ import annotations

import grpc
from grpc.aio import Metadata
from google.protobuf import any_pb2
from google.protobuf.message import Message
from google.rpc import status_pb2
from grpc_status.rpc_status import to_status

from cadence.api.v1 import error_pb2


class ServiceError(Exception):
    def __init__(
        self, code: grpc.StatusCode, message: str, detail: Message | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail

    def trailing_metadata(self) -> Metadata:
        status = status_pb2.Status(code=self.code.value[0], message=self.message)
        if self.detail is not None:
            packed = any_pb2.Any()
            packed.Pack(self.detail)
            status.details.append(packed)
        return Metadata(*to_status(status).trailing_metadata)


def bad_request(message: str) -> ServiceError:
    return ServiceError(grpc.StatusCode.INVALID_ARGUMENT, message)


def entity_not_exists(message: str) -> ServiceError:
    return ServiceError(
        grpc.StatusCode.NOT_FOUND, message, error_pb2.EntityNotExistsError()
    )


def already_completed() -> ServiceError:
    return ServiceError(
        grpc.StatusCode.NOT_FOUND,
        "workflow execution already completed",
        error_pb2.WorkflowExecutionAlreadyCompletedError(),
    )


def already_started(start_request_id: str, run_id: str) -> ServiceError:
    return ServiceError(
        grpc.StatusCode.ALREADY_EXISTS,
        "Workflow execution is already running",
        error_pb2.WorkflowExecutionAlreadyStartedError(
            start_request_id=start_request_id, run_id=run_id
        ),
    )


def cancellation_already_requested() -> ServiceError:
    return ServiceError(
        grpc.StatusCode.ALREADY_EXISTS,
        "Cancellation already requested for this workflow execution",
        error_pb2.CancellationAlreadyRequestedError(),
    )


def domain_already_exists(name: str) -> ServiceError:
    return ServiceError(
        grpc.StatusCode.ALREADY_EXISTS,
        f"Domain already exists: {name}",
        error_pb2.DomainAlreadyExistsError(),
    )


def query_failed(message: str) -> ServiceError:
    return ServiceError(
        grpc.StatusCode.INVALID_ARGUMENT, message, error_pb2.QueryFailedError()
    )


def unimplemented(message: str) -> ServiceError:
    return ServiceError(grpc.StatusCode.UNIMPLEMENTED, message)
