from __future__ import annotations
import time
from typing import TYPE_CHECKING, Any, overload
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from prefect.client.schemas.objects import StateType
from prefect.deployments import run_deployment

from infrahub.workflows.initialization import setup_task_manager
from infrahub.workflows.models import WorkflowInfo

from . import InfrahubWorkflow, Return

if TYPE_CHECKING:
    from prefect.client.schemas.objects import FlowRun
    from infrahub.services import InfrahubServices
    from infrahub.workflows.models import WorkflowDefinition

class WorkflowWorkerExecution(InfrahubWorkflow):
    async def initialize(self, service: InfrahubServices) -> None:
        """Initialize the Workflow engine"""
        with trace.get_tracer(__name__).start_as_current_span(
            "prefect.initialize",
            kind=SpanKind.INTERNAL,
            attributes={
                "service.name": "infrahub",
                "peer.service": "prefect",
                "operation": "initialize",
                "component.type": "workflow"
            }
        ) as span:
            try:
                if await service.component.is_primary_api():
                    await setup_task_manager()
                span.set_attribute("initialized", True)
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    @overload
    async def execute_workflow(
        self,
        workflow: WorkflowDefinition,
        expected_return: type[Return],
        parameters: dict[str, Any] | None = ...,
        tags: list[str] | None = ...,
    ) -> Return: ...

    @overload
    async def execute_workflow(
        self,
        workflow: WorkflowDefinition,
        expected_return: None = ...,
        parameters: dict[str, Any] | None = ...,
        tags: list[str] | None = ...,
    ) -> Any: ...

    async def execute_workflow(
        self,
        workflow: WorkflowDefinition,
        expected_return: type[Return] | None = None,
        parameters: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        with trace.get_tracer(__name__).start_as_current_span(
            "prefect.execute_workflow",
            kind=SpanKind.CONSUMER,
            attributes={
                "service.name": "infrahub",
                "peer.service": "prefect",
                "workflow.name": workflow.full_name,
                "workflow.parameters": str(parameters),
                "workflow.tags": str(tags),
                "span.status": "started"
            }
        ) as span:
            try:
                start_time = time.time()
                response: FlowRun = await run_deployment(
                    name=workflow.full_name, 
                    poll_interval=1, 
                    parameters=parameters or {}, 
                    tags=tags
                )

                if not response.state:
                    span.set_attribute("error.type", "NoState")
                    raise RuntimeError("Unable to read state from the response")

                span.set_attribute("workflow.state", response.state.type.value)
                if response.state.type == StateType.CRASHED:
                    span.set_attribute("error.type", "WorkflowCrashed")
                    span.set_attribute("error.message", response.state.message)
                    raise RuntimeError(response.state.message)

                result = await response.state.result(raise_on_failure=True, fetch=True)
                
                duration = time.time() - start_time
                span.set_attribute("duration_ms", duration * 1000)
                span.set_attribute("span.status", "success")
                
                return result

            except Exception as e:
                span.record_exception(e)
                span.set_attribute("span.status", "error")
                span.set_attribute("error.type", e.__class__.__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise

    async def submit_workflow(
        self, 
        workflow: WorkflowDefinition, 
        parameters: dict[str, Any] | None = None, 
        tags: list[str] | None = None
    ) -> WorkflowInfo:
        with trace.get_tracer(__name__).start_as_current_span(
            "prefect.submit_workflow",
            kind=SpanKind.PRODUCER,
            attributes={
                "service.name": "infrahub",
                "peer.service": "prefect",
                "workflow.name": workflow.full_name,
                "workflow.parameters": str(parameters),
                "workflow.tags": str(tags),
                "span.status": "started"
            }
        ) as span:
            try:
                start_time = time.time()
                flow_run = await run_deployment(
                    name=workflow.full_name, 
                    timeout=0, 
                    parameters=parameters or {}, 
                    tags=tags
                )
                
                workflow_info = WorkflowInfo.from_flow(flow_run=flow_run)
                
                duration = time.time() - start_time
                span.set_attribute("duration_ms", duration * 1000)
                span.set_attribute("workflow.run_id", str(flow_run.id))
                span.set_attribute("span.status", "success")
                
                return workflow_info
                
            except Exception as e:
                span.record_exception(e)
                span.set_attribute("span.status", "error")
                span.set_attribute("error.type", e.__class__.__name__)
                span.set_attribute("error.message", str(e))
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise