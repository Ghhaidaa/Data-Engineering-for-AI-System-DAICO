"""
OpenLineage event emitter used across all pipeline stages
(Ingestion, Lakehouse, Quality Gate, RAG) to record START/COMPLETE/FAIL events.
"""
from openlineage.client import OpenLineageClient
from openlineage.client.transport.console import ConsoleConfig, ConsoleTransport
from openlineage.client.run import RunEvent, RunState, Run, Job
from datetime import datetime, timezone
import uuid

ol_client = OpenLineageClient(transport=ConsoleTransport(ConsoleConfig()))


def emit_lineage_event(job_name: str, event_type: str, run_id: str = None, facets: dict = None):
    """
    Emit a real OpenLineage RunEvent.
    event_type must be one of: START, COMPLETE, FAIL
    """
    run_id = run_id or str(uuid.uuid4())
    event = RunEvent(
        eventType=getattr(RunState, event_type),
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id),
        job=Job(namespace="sdaia-books-platform", name=job_name),
        producer="sdaia-books-platform-pipeline",
    )
    ol_client.emit(event)
    return run_id
