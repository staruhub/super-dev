"""
Plan-Execute engine for Super Dev.

Description: Scheduler + Wave style plan execution with topological wave
sorting, verification gates, persistence, and optional Codex review support.
Created: 2026-04-10
"""

from __future__ import annotations

import json
import logging
import shlex
import subprocess
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence


LOGGER = logging.getLogger(__name__)
SUPPORTED_EXECUTORS = {"claude-code", "codex", "auto"}
ALLOWED_GATE_COMMANDS: set[str] = {
    "npm",
    "npx",
    "node",
    "python",
    "python3",
    "pytest",
    "pip",
    "go",
    "cargo",
    "rustc",
    "javac",
    "java",
    "mvn",
    "gradle",
    "make",
    "cmake",
    "tsc",
    "eslint",
    "prettier",
    "jest",
    "vitest",
    "ruff",
    "mypy",
    "black",
    "flake8",
    "pylint",
    "isort",
    "super-dev",
    "git",
    "docker",
    "kubectl",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _serialize_datetime(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _deserialize_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _truncate_output(value: str | bytes | None, limit: int = 2000) -> str:
    if value is None:
        return ""
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
    return text[:limit]


def _extract_json_object(raw_output: str) -> dict[str, Any] | None:
    text = raw_output.strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}")
        if start < 0 or end < start:
            return None
        try:
            parsed = json.loads(text[start : end + 1])
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class StepStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    VERIFYING = "verifying"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"

    def can_transition_to(self, target: StepStatus) -> bool:
        transitions: dict[StepStatus, set[StepStatus]] = {
            StepStatus.PENDING: {StepStatus.QUEUED, StepStatus.BLOCKED, StepStatus.SKIPPED},
            StepStatus.BLOCKED: {StepStatus.QUEUED, StepStatus.SKIPPED},
            StepStatus.QUEUED: {StepStatus.RUNNING, StepStatus.BLOCKED, StepStatus.SKIPPED},
            StepStatus.RUNNING: {StepStatus.VERIFYING, StepStatus.FAILED, StepStatus.COMPLETED},
            StepStatus.VERIFYING: {StepStatus.COMPLETED, StepStatus.FAILED},
            StepStatus.FAILED: {StepStatus.QUEUED},
            StepStatus.COMPLETED: set(),
            StepStatus.SKIPPED: set(),
        }
        return target in transitions.get(self, set())


@dataclass
class VerifyGate:
    command: str
    gate: str
    required: bool = True
    timeout_seconds: int = 120


@dataclass
class PlanStep:
    id: str
    label: str
    description: str
    phase: str
    executor: str = "claude-code"
    depends_on: list[str] = field(default_factory=list)
    verify_gates: list[VerifyGate] = field(default_factory=list)
    allowed_files: list[str] = field(default_factory=list)
    forbidden_files: list[str] = field(default_factory=list)
    status: StepStatus = StepStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    duration_seconds: float | None = None
    output: str = ""
    errors: list[str] = field(default_factory=list)
    failure_count: int = 0
    failure_budget: int = 3
    codex_review: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.executor not in SUPPORTED_EXECUTORS:
            raise ValueError(f"Unsupported executor: {self.executor}")
        if self.failure_budget < 1:
            raise ValueError("failure_budget must be at least 1")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "phase": self.phase,
            "executor": self.executor,
            "depends_on": list(self.depends_on),
            "verify_gates": [
                {
                    "command": gate.command,
                    "gate": gate.gate,
                    "required": gate.required,
                    "timeout_seconds": gate.timeout_seconds,
                }
                for gate in self.verify_gates
            ],
            "allowed_files": list(self.allowed_files),
            "forbidden_files": list(self.forbidden_files),
            "status": self.status.value,
            "started_at": _serialize_datetime(self.started_at),
            "completed_at": _serialize_datetime(self.completed_at),
            "duration_seconds": self.duration_seconds,
            "output": self.output,
            "errors": list(self.errors),
            "failure_count": self.failure_count,
            "failure_budget": self.failure_budget,
            "codex_review": self.codex_review,
        }


@dataclass
class ExecutionPlan:
    plan_id: str
    phase: str
    created_at: datetime = field(default_factory=_utcnow)
    steps: list[PlanStep] = field(default_factory=list)
    waves: list[list[str]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "phase": self.phase,
            "created_at": _serialize_datetime(self.created_at),
            "steps": [step.to_dict() for step in self.steps],
            "waves": [list(wave) for wave in self.waves],
            "metadata": dict(self.metadata),
        }

    def get_step(self, step_id: str) -> PlanStep | None:
        return next((step for step in self.steps if step.id == step_id), None)

    @property
    def progress(self) -> dict[str, int]:
        return {
            "total": len(self.steps),
            "completed": sum(1 for step in self.steps if step.status == StepStatus.COMPLETED),
            "failed": sum(1 for step in self.steps if step.status == StepStatus.FAILED),
            "running": sum(
                1
                for step in self.steps
                if step.status in {StepStatus.RUNNING, StepStatus.VERIFYING}
            ),
        }

    @property
    def all_completed(self) -> bool:
        return all(step.status in {StepStatus.COMPLETED, StepStatus.SKIPPED} for step in self.steps)


class PlanExecutor:
    def __init__(self, project_dir: str | Path):
        self.project_dir = Path(project_dir).resolve()
        self.plans_dir = self.project_dir / ".super-dev" / "plans"
        self.plans_dir.mkdir(parents=True, exist_ok=True)

    def create_plan(
        self,
        phase: str,
        steps: Sequence[PlanStep | Mapping[str, Any]],
        metadata: dict[str, Any] | None = None,
    ) -> ExecutionPlan:
        created_at = _utcnow()
        plan_id = f"{phase}-{created_at.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        plan_steps = [self._coerce_step(phase, raw_step) for raw_step in steps]
        plan = ExecutionPlan(
            plan_id=plan_id,
            phase=phase,
            created_at=created_at,
            steps=plan_steps,
            metadata=dict(metadata or {}),
        )
        plan.waves = self._topological_sort(plan.steps)
        self._save_plan(plan)
        LOGGER.info("Created plan %s with %s step(s)", plan.plan_id, len(plan.steps))
        return plan

    def transition_step(self, plan: ExecutionPlan, step_id: str, new_status: StepStatus) -> bool:
        step = plan.get_step(step_id)
        if step is None:
            LOGGER.warning("Cannot transition unknown step %s in plan %s", step_id, plan.plan_id)
            return False
        if not step.status.can_transition_to(new_status):
            LOGGER.warning(
                "Invalid step transition for %s: %s -> %s",
                step_id,
                step.status.value,
                new_status.value,
            )
            return False

        now = _utcnow()
        previous_status = step.status
        step.status = new_status

        if new_status == StepStatus.RUNNING:
            step.started_at = now
            step.completed_at = None
            step.duration_seconds = None
        elif new_status in {StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED}:
            step.completed_at = now
            if step.started_at is not None:
                step.duration_seconds = round((now - step.started_at).total_seconds(), 3)
        elif new_status == StepStatus.QUEUED and previous_status == StepStatus.FAILED:
            step.started_at = None
            step.completed_at = None
            step.duration_seconds = None

        self._save_plan(plan)
        LOGGER.info(
            "Transitioned step %s in plan %s: %s -> %s",
            step_id,
            plan.plan_id,
            previous_status.value,
            new_status.value,
        )
        return True

    def get_next_ready_steps(self, plan: ExecutionPlan) -> list[PlanStep]:
        step_lookup = {step.id: step for step in plan.steps}
        ready_steps: list[PlanStep] = []
        for step in plan.steps:
            if step.status not in {StepStatus.PENDING, StepStatus.QUEUED}:
                continue
            if all(
                dependency_id in step_lookup
                and step_lookup[dependency_id].status == StepStatus.COMPLETED
                for dependency_id in step.depends_on
            ):
                ready_steps.append(step)
        return ready_steps

    @staticmethod
    def _validate_gate_command(command: str) -> tuple[list[str], str]:
        forbidden_chars = {"|", ";", "&", "`", "$", "(", ")", "<", ">", "\n"}
        if any(char in command for char in forbidden_chars):
            return [], "Command contains forbidden shell metacharacters"
        try:
            argv = shlex.split(command)
        except ValueError as exc:
            return [], f"Invalid command syntax: {exc}"
        if not argv:
            return [], "Command is empty"
        executable = argv[0].split("/")[-1]
        if executable not in ALLOWED_GATE_COMMANDS:
            return [], f"Executable '{executable}' is not in the allowlist"
        return argv, ""

    def run_verify_gates(
        self,
        step: PlanStep,
        work_dir: str | Path | None = None,
    ) -> tuple[bool, list[dict[str, Any]]]:
        cwd = Path(work_dir).resolve() if work_dir else self.project_dir
        results: list[dict[str, Any]] = []
        all_required_passed = True

        for gate in step.verify_gates:
            started = time.monotonic()
            argv, validation_error = self._validate_gate_command(gate.command)
            if validation_error:
                result = {
                    "gate": gate.gate,
                    "command": gate.command,
                    "required": gate.required,
                    "timeout_seconds": gate.timeout_seconds,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "passed": False,
                    "exit_code": -1,
                    "stdout": "",
                    "stderr": "",
                    "error": f"Verify gate command rejected: {validation_error}",
                }
                if gate.required:
                    all_required_passed = False
                results.append(result)
                continue
            try:
                completed = subprocess.run(
                    argv,
                    shell=False,
                    cwd=str(cwd),
                    capture_output=True,
                    text=True,
                    timeout=gate.timeout_seconds,
                )
                passed = completed.returncode == 0
                result = {
                    "gate": gate.gate,
                    "command": gate.command,
                    "required": gate.required,
                    "timeout_seconds": gate.timeout_seconds,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "passed": passed,
                    "exit_code": completed.returncode,
                    "stdout": _truncate_output(completed.stdout),
                    "stderr": _truncate_output(completed.stderr),
                }
            except subprocess.TimeoutExpired as exc:
                result = {
                    "gate": gate.gate,
                    "command": gate.command,
                    "required": gate.required,
                    "timeout_seconds": gate.timeout_seconds,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "passed": False,
                    "exit_code": None,
                    "stdout": _truncate_output(exc.stdout),
                    "stderr": _truncate_output(exc.stderr),
                    "error": f"Timed out after {gate.timeout_seconds} seconds",
                }
            except OSError as exc:
                result = {
                    "gate": gate.gate,
                    "command": gate.command,
                    "required": gate.required,
                    "timeout_seconds": gate.timeout_seconds,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "passed": False,
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "",
                    "error": str(exc),
                }

            if gate.required and not result["passed"]:
                all_required_passed = False
            results.append(result)

        return all_required_passed, results

    def request_codex_review(self, step: PlanStep, context: str) -> dict[str, Any]:
        prompt = (
            f"Review step '{step.id}' ({step.label}) in phase '{step.phase}'.\n\n"
            f"Description:\n{step.description}\n\n"
            f"Context:\n{context}\n\n"
            "Return JSON only with keys passed, severity, issues, summary."
        )

        try:
            completed = subprocess.run(
                ["codex", "--quiet", "--prompt", prompt],
                cwd=str(self.project_dir),
                capture_output=True,
                text=True,
                timeout=300,
            )
        except FileNotFoundError:
            return {
                "requested": True,
                "available": False,
                "passed": True,
                "severity": "none",
                "error": "codex CLI not found",
                "timestamp": _serialize_datetime(_utcnow()),
            }
        except subprocess.TimeoutExpired:
            return {
                "requested": True,
                "available": True,
                "passed": True,
                "severity": "none",
                "error": "codex review timed out after 300 seconds",
                "timestamp": _serialize_datetime(_utcnow()),
            }
        except OSError as exc:
            return {
                "requested": True,
                "available": True,
                "passed": True,
                "severity": "none",
                "error": str(exc),
                "timestamp": _serialize_datetime(_utcnow()),
            }

        raw_output = (completed.stdout or "").strip()
        parsed = _extract_json_object(raw_output)
        review: dict[str, Any] = {
            "requested": True,
            "available": True,
            "exit_code": completed.returncode,
            "raw_output": _truncate_output(raw_output, 4000),
            "stderr": _truncate_output(completed.stderr, 1000),
            "timestamp": _serialize_datetime(_utcnow()),
            "passed": completed.returncode == 0,
            "severity": "unknown",
        }
        if parsed is not None:
            review["parsed"] = parsed
            review["passed"] = bool(parsed.get("passed", completed.returncode == 0))
            review["severity"] = str(parsed.get("severity", "none"))
        return review

    def handle_step_failure(self, plan: ExecutionPlan, step: PlanStep, error: str) -> str:
        step.failure_count += 1
        step.errors.append(error)
        step.status = StepStatus.FAILED
        step.completed_at = _utcnow()
        if step.started_at is not None:
            step.duration_seconds = round((step.completed_at - step.started_at).total_seconds(), 3)

        if step.failure_count < step.failure_budget:
            step.status = StepStatus.QUEUED
            step.started_at = None
            step.completed_at = None
            step.duration_seconds = None
            self._save_plan(plan)
            LOGGER.warning(
                "Step %s failed in plan %s and will retry (%s/%s)",
                step.id,
                plan.plan_id,
                step.failure_count,
                step.failure_budget,
            )
            return "retry"

        self._save_plan(plan)
        LOGGER.error(
            "Step %s failed in plan %s and exhausted failure budget (%s/%s)",
            step.id,
            plan.plan_id,
            step.failure_count,
            step.failure_budget,
        )
        return "needs_human"

    def load_plan(self, plan_id: str) -> ExecutionPlan | None:
        plan_path = self.plans_dir / f"{plan_id}.json"
        if not plan_path.exists():
            return None
        try:
            payload = json.loads(plan_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            LOGGER.exception("Failed to load plan from %s", plan_path)
            return None
        try:
            plan = self._dict_to_plan(payload)
        except (KeyError, TypeError, ValueError):
            LOGGER.exception("Failed to deserialize plan from %s", plan_path)
            return None
        LOGGER.info(
            "Loaded plan %s from %s; verify gates will be validated against the command allowlist",
            plan.plan_id,
            plan_path,
        )
        return plan

    def list_plans(self) -> list[dict[str, Any]]:
        plans: list[dict[str, Any]] = []
        for plan_path in sorted(self.plans_dir.glob("*.json")):
            try:
                payload = json.loads(plan_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                LOGGER.warning("Skipping unreadable plan file %s", plan_path)
                continue
            plans.append(
                {
                    "plan_id": payload.get("plan_id", plan_path.stem),
                    "phase": payload.get("phase", ""),
                    "created_at": payload.get("created_at"),
                    "step_count": len(payload.get("steps", [])),
                    "wave_count": len(payload.get("waves", [])),
                    "metadata": payload.get("metadata", {}),
                }
            )
        return plans

    def generate_plan_report(self, plan: ExecutionPlan) -> str:
        progress = plan.progress
        lines = [
            f"# Plan Report: {plan.plan_id}",
            "",
            f"- Phase: `{plan.phase}`",
            f"- Created At: `{_serialize_datetime(plan.created_at)}`",
            f"- Progress: `{progress['completed']}/{progress['total']}` completed, "
            f"`{progress['failed']}` failed, `{progress['running']}` running",
            "",
            "## Waves",
            "",
        ]

        for index, wave in enumerate(plan.waves, start=1):
            lines.append(f"### Wave {index}")
            lines.append("| Step ID | Label | Status | Executor | Duration |")
            lines.append("| --- | --- | --- | --- | --- |")
            for step_id in wave:
                step = plan.get_step(step_id)
                if step is None:
                    continue
                duration = f"{step.duration_seconds:.3f}s" if step.duration_seconds is not None else "-"
                lines.append(
                    f"| `{step.id}` | {step.label} | `{step.status.value}` | "
                    f"`{step.executor}` | {duration} |"
                )
            lines.append("")

        lines.extend(["## Step Details", ""])
        for step in plan.steps:
            lines.append(f"### {step.label} (`{step.id}`)")
            lines.append(f"- Status: `{step.status.value}`")
            lines.append(f"- Description: {step.description or '-'}")
            lines.append(f"- Depends On: {', '.join(step.depends_on) if step.depends_on else '-'}")
            lines.append(f"- Allowed Files: {', '.join(step.allowed_files) if step.allowed_files else '-'}")
            lines.append(
                f"- Forbidden Files: {', '.join(step.forbidden_files) if step.forbidden_files else '-'}"
            )
            lines.append(
                f"- Started At: `{_serialize_datetime(step.started_at) or '-'}`"
            )
            lines.append(
                f"- Completed At: `{_serialize_datetime(step.completed_at) or '-'}`"
            )
            lines.append(
                f"- Failure Budget: `{step.failure_count}/{step.failure_budget}`"
            )
            if step.errors:
                lines.append(f"- Latest Error: {step.errors[-1]}")
            if step.codex_review:
                lines.append(
                    f"- Codex Review: `{step.codex_review.get('severity', 'unknown')}` / "
                    f"`passed={step.codex_review.get('passed', True)}`"
                )
            lines.append("")

        return "\n".join(lines).rstrip() + "\n"

    def _topological_sort(self, steps: Sequence[PlanStep]) -> list[list[str]]:
        step_order = [step.id for step in steps]
        if len(step_order) != len(set(step_order)):
            raise ValueError("Duplicate step ids are not allowed in a plan")

        in_degree: dict[str, int] = {step.id: 0 for step in steps}
        dependents: dict[str, list[str]] = defaultdict(list)

        for step in steps:
            for dependency_id in step.depends_on:
                if dependency_id not in in_degree:
                    raise ValueError(f"Unknown dependency '{dependency_id}' for step '{step.id}'")
                in_degree[step.id] += 1
                dependents[dependency_id].append(step.id)

        queue: deque[str] = deque(step_id for step_id in step_order if in_degree[step_id] == 0)
        waves: list[list[str]] = []
        visited_count = 0

        while queue:
            current_wave = list(queue)
            queue.clear()
            waves.append(current_wave)
            next_wave: list[str] = []

            for step_id in current_wave:
                visited_count += 1
                for dependent_id in dependents.get(step_id, []):
                    in_degree[dependent_id] -= 1
                    if in_degree[dependent_id] == 0:
                        next_wave.append(dependent_id)

            for step_id in step_order:
                if step_id in next_wave:
                    queue.append(step_id)

        if visited_count != len(steps):
            unresolved = [step_id for step_id, degree in in_degree.items() if degree > 0]
            raise ValueError(f"Cycle detected while building plan waves: {', '.join(unresolved)}")
        return waves

    def _save_plan(self, plan: ExecutionPlan) -> None:
        plan_path = self.plans_dir / f"{plan.plan_id}.json"
        plan_path.write_text(
            json.dumps(plan.to_dict(), indent=2, ensure_ascii=False, default=_json_default) + "\n",
            encoding="utf-8",
        )

    def _dict_to_plan(self, data: Mapping[str, Any]) -> ExecutionPlan:
        steps = [self._coerce_step(str(data["phase"]), raw_step) for raw_step in data.get("steps", [])]
        return ExecutionPlan(
            plan_id=str(data["plan_id"]),
            phase=str(data["phase"]),
            created_at=_deserialize_datetime(data.get("created_at")) or _utcnow(),
            steps=steps,
            waves=[list(wave) for wave in data.get("waves", [])],
            metadata=dict(data.get("metadata", {})),
        )

    def _coerce_step(self, phase: str, raw_step: PlanStep | Mapping[str, Any]) -> PlanStep:
        if isinstance(raw_step, PlanStep):
            return raw_step

        verify_gates = [
            VerifyGate(
                command=str(gate["command"]),
                gate=str(gate["gate"]),
                required=bool(gate.get("required", True)),
                timeout_seconds=int(gate.get("timeout_seconds", 120)),
            )
            for gate in raw_step.get("verify_gates", [])
        ]
        status_value = raw_step.get("status", StepStatus.PENDING)
        status = status_value if isinstance(status_value, StepStatus) else StepStatus(str(status_value))

        return PlanStep(
            id=str(raw_step["id"]),
            label=str(raw_step["label"]),
            description=str(raw_step.get("description", "")),
            phase=str(raw_step.get("phase", phase)),
            executor=str(raw_step.get("executor", "claude-code")),
            depends_on=[str(dep) for dep in raw_step.get("depends_on", [])],
            verify_gates=verify_gates,
            allowed_files=[str(path) for path in raw_step.get("allowed_files", [])],
            forbidden_files=[str(path) for path in raw_step.get("forbidden_files", [])],
            status=status,
            started_at=_deserialize_datetime(raw_step.get("started_at")),
            completed_at=_deserialize_datetime(raw_step.get("completed_at")),
            duration_seconds=(
                float(raw_step["duration_seconds"])
                if raw_step.get("duration_seconds") is not None
                else None
            ),
            output=str(raw_step.get("output", "")),
            errors=[str(item) for item in raw_step.get("errors", [])],
            failure_count=int(raw_step.get("failure_count", 0)),
            failure_budget=int(raw_step.get("failure_budget", 3)),
            codex_review=(
                dict(raw_step["codex_review"])
                if isinstance(raw_step.get("codex_review"), Mapping)
                else raw_step.get("codex_review")
            ),
        )
