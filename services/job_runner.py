from __future__ import annotations

from typing import Any

from services.orchestrator import IngestionOrchestrator


class JobRunner:
    def __init__(self, orchestrator: IngestionOrchestrator | None = None) -> None:
        self.orchestrator = orchestrator or IngestionOrchestrator()

    def run(self, job_name: str) -> Any:
        return self.orchestrator.run(job_name)

