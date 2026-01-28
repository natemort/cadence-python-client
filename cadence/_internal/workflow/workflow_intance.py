import logging
import traceback
from asyncio import CancelledError, InvalidStateError, Task
from typing import Any, Optional, Callable, Awaitable
from cadence._internal.workflow.deterministic_event_loop import (
    DeterministicEventLoop,
    FatalDecisionError,
)
from cadence.api.v1.common_pb2 import Payload, Failure
from cadence.api.v1.decision_pb2 import (
    CompleteWorkflowExecutionDecisionAttributes,
    FailWorkflowExecutionDecisionAttributes,
    Decision,
)

from cadence.data_converter import DataConverter
from cadence.workflow import WorkflowDefinition

logger = logging.getLogger(__name__)


class WorkflowInstance:
    def __init__(
        self,
        loop: DeterministicEventLoop,
        workflow_definition: WorkflowDefinition,
        data_converter: DataConverter,
    ):
        self._loop = loop
        self._definition = workflow_definition
        self._data_converter = data_converter
        self._instance = workflow_definition.cls()  # construct a new workflow object
        self._task: Optional[Task[Decision]] = None
        self._unhandled_exceptions: list[BaseException] = []

    def start(self, payload: Payload):
        if self._task is None:
            run_method = self._definition.get_run_method(self._instance)
            # noinspection PyProtectedMember
            workflow_input = self._definition._run_signature.params_from_payload(
                self._data_converter, payload
            )

            self._task = self._loop.create_task(self._run(run_method, workflow_input))
            self._task.add_done_callback(self._on_task_done)

    async def _run(
        self, workflow_fn: Callable[[Any], Awaitable[Any]], args: list[Any]
    ) -> Decision:
        try:
            result = await workflow_fn(*args)
            as_payload = self._data_converter.to_data([result])
            return Decision(
                complete_workflow_execution_decision_attributes=CompleteWorkflowExecutionDecisionAttributes(
                    result=as_payload
                )
            )
        except (CancelledError, InvalidStateError, FatalDecisionError):
            raise
        except ExceptionGroup as e:
            if e.subgroup((InvalidStateError, FatalDecisionError)):
                raise
            failure = _failure_from_exception(e)
            return Decision(
                fail_workflow_execution_decision_attributes=FailWorkflowExecutionDecisionAttributes(
                    failure=failure
                )
            )
        except Exception as e:
            failure = _failure_from_exception(e)
            return Decision(
                fail_workflow_execution_decision_attributes=FailWorkflowExecutionDecisionAttributes(
                    failure=failure
                )
            )

    def _on_task_done(self, task: Task) -> None:
        exception = task.exception()
        if exception is not None:
            self._unhandled_exceptions.append(exception)

    def run_until_yield(self):
        self._loop.run_until_yield()
        if len(self._unhandled_exceptions) == 1:
            raise self._unhandled_exceptions[0]
        elif len(self._unhandled_exceptions) > 1:
            raise BaseExceptionGroup(
                "Unhandled Exceptions in Workflow", self._unhandled_exceptions
            )

    def is_done(self) -> bool:
        return self._task is not None and self._task.done()

    def get_result(self) -> Optional[Decision]:
        if self._task is None or not self._task.done():
            return None
        return self._task.result()


def _failure_from_exception(e: Exception) -> Failure:
    stacktrace = "".join(traceback.format_exception(e))

    details = f"message: {str(e)}\nstacktrace: {stacktrace}"

    return Failure(
        reason=type(e).__name__,
        details=details.encode("utf-8"),
    )
