from __future__ import annotations

from typing import TYPE_CHECKING, Any, overload

from opentelemetry import propagate
from opentelemetry.instrumentation.utils import is_instrumentation_enabled
from prefect.client.orchestration._deployments.client import DeploymentAsyncClient
from prefect.client.schemas.objects import StateType
from prefect.deployments import run_deployment
from prefect.telemetry.run_telemetry import LABELS_TRACEPARENT_KEY, TRACEPARENT_KEY, OTELSetter

from infrahub.workflows.initialization import setup_task_manager
from infrahub.workflows.models import WorkflowInfo

from . import InfrahubWorkflow, Return

if TYPE_CHECKING:
    from collections.abc import Iterable

    from prefect.client.schemas.objects import FlowRun
    from prefect.states import State
    from prefect.types import KeyValueLabels, KeyValueLabelsField

    from infrahub.workflows.models import WorkflowDefinition

old_func = DeploymentAsyncClient.create_flow_run_from_deployment


async def create_flow_run_from_deployment(
    self,
    deployment_id: "UUID",
    *,
    parameters: dict[str, Any] | None = None,
    context: dict[str, Any] | None = None,
    state: State[Any] | None = None,
    name: str | None = None,
    tags: Iterable[str] | None = None,
    idempotency_key: str | None = None,
    parent_task_run_id: "UUID | None" = None,
    work_queue_name: str | None = None,
    job_variables: dict[str, Any] | None = None,
    labels: KeyValueLabelsField | None = None,
) -> "FlowRun":
    if not labels:
        if is_instrumentation_enabled():
            carrier: KeyValueLabels = {}
            propagate.get_global_textmap().inject(
                carrier,
                setter=OTELSetter(),
            )
            if carrier.get(TRACEPARENT_KEY):
                labels = {LABELS_TRACEPARENT_KEY: carrier[TRACEPARENT_KEY]}

    return await old_func(
        self=self,
        deployment_id=deployment_id,
        parameters=parameters,
        context=context,
        state=state,
        name=name,
        tags=tags,
        idempotency_key=idempotency_key,
        parent_task_run_id=parent_task_run_id,
        work_queue_name=work_queue_name,
        job_variables=job_variables,
        labels=labels,
    )


DeploymentAsyncClient.create_flow_run_from_deployment = create_flow_run_from_deployment


class WorkflowWorkerExecution(InfrahubWorkflow):
    @staticmethod
    async def initialize(component_is_primary_server: bool) -> None:
        if component_is_primary_server:
            await setup_task_manager()

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

    # TODO Make expected_return mandatory and remove above overloads.
    async def execute_workflow(
        self,
        workflow: WorkflowDefinition,
        expected_return: type[Return] | None = None,
        parameters: dict[str, Any] | None = None,
        tags: list[str] | None = None,
    ) -> Any:
        response: FlowRun = await run_deployment(
            name=workflow.full_name, poll_interval=1, parameters=parameters or {}, tags=tags
        )  # type: ignore[return-value, misc]
        if not response.state:
            raise RuntimeError("Unable to read state from the response")

        if response.state.type == StateType.CRASHED:
            raise RuntimeError(response.state.message)

        return await response.state.result(raise_on_failure=True, fetch=True)  # type: ignore[call-overload]

    async def submit_workflow(
        self, workflow: WorkflowDefinition, parameters: dict[str, Any] | None = None, tags: list[str] | None = None
    ) -> WorkflowInfo:
        flow_run = await run_deployment(name=workflow.full_name, timeout=0, parameters=parameters or {}, tags=tags)  # type: ignore[return-value, misc]
        return WorkflowInfo.from_flow(flow_run=flow_run)
