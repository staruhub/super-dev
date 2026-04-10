"""
开发：Excellent（11964948@qq.com）
功能：工作流编排引擎 - 协调 12 阶段工作流（含第 0 阶段）
作用：管理任务执行、专家调度、质量门禁
创建时间：2025-12-30
最后修改：2026-03-20
"""

import asyncio
import json
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

try:
    from rich.panel import Panel
    from rich.table import Table

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

from ..config.manager import ConfigManager, get_config_manager
from ..exceptions import PhaseExecutionError, QualityGateError
from ..terminal import create_console
from ..utils import get_logger

try:
    from .knowledge_pusher import KnowledgePusher

    KNOWLEDGE_PUSHER_AVAILABLE = True
except ImportError:
    KNOWLEDGE_PUSHER_AVAILABLE = False

try:
    from .governance import PipelineGovernance

    GOVERNANCE_AVAILABLE = True
except ImportError:
    GOVERNANCE_AVAILABLE = False

try:
    from ..hooks.manager import HookEvent, HookManager

    HOOKS_AVAILABLE = True
except ImportError:
    HOOKS_AVAILABLE = False

try:
    from .context_compact import CompactEngine

    COMPACT_AVAILABLE = True
except ImportError:
    COMPACT_AVAILABLE = False

try:
    from ..memory.consolidator import MemoryConsolidator
    from ..memory.extractor import MemoryExtractor
    from ..memory.store import MemoryStore

    MEMORY_AVAILABLE = True
except ImportError:
    MEMORY_AVAILABLE = False

try:
    from ..session.brief import SessionBrief

    SESSION_BRIEF_AVAILABLE = True
except ImportError:
    SESSION_BRIEF_AVAILABLE = False

try:
    from ..pipeline_cost import PipelineCostTracker

    COST_TRACKER_AVAILABLE = True
except ImportError:
    COST_TRACKER_AVAILABLE = False

try:
    from .plan_executor import PlanExecutor
    PLAN_EXECUTOR_AVAILABLE = True
except ImportError:
    PLAN_EXECUTOR_AVAILABLE = False

try:
    from .overseer import Overseer
    OVERSEER_AVAILABLE = True
except ImportError:
    OVERSEER_AVAILABLE = False


class Phase(Enum):
    """工作流阶段"""

    DISCOVERY = "discovery"
    INTELLIGENCE = "intelligence"
    DRAFTING = "drafting"
    REDTEAM = "redteam"
    QA = "qa"
    DELIVERY = "delivery"
    DEPLOYMENT = "deployment"


@dataclass
class PhaseResult:
    """阶段执行结果"""

    phase: Phase
    success: bool
    duration: float
    output: Any = None
    errors: list = field(default_factory=list)
    quality_score: float = 0.0


@dataclass
class WorkflowContext:
    """工作流上下文"""

    project_dir: Path
    config: ConfigManager
    results: dict = field(default_factory=dict)
    metadata: dict = field(default_factory=dict)

    # 共享数据
    user_input: dict = field(default_factory=dict)
    research_data: dict = field(default_factory=dict)
    documents: dict = field(default_factory=dict)
    quality_reports: dict = field(default_factory=dict)


class WorkflowEngine:
    """工作流编排引擎"""

    def __init__(self, project_dir: Path | None = None):
        self.project_dir = Path.cwd() if project_dir is None else project_dir
        self.config_manager = get_config_manager(self.project_dir)
        self.console = create_console() if RICH_AVAILABLE else None
        self.logger = get_logger(
            "workflow_engine", level="INFO", log_file=self.project_dir / "logs" / "workflow.log"
        )

        # 阶段注册表
        self._phase_handlers: dict[Phase, Callable] = {}
        self._checkpoint_dir = self.project_dir / ".super-dev" / "checkpoints"

        # 注册默认阶段处理器
        self._register_default_handlers()

        # 初始化治理层（可选，不影响 pipeline 正常运行）
        self.governance = None
        if GOVERNANCE_AVAILABLE:
            try:
                self.governance = PipelineGovernance(self.project_dir)
            except Exception as e:
                self.logger.warning(f"治理层初始化失败，pipeline 将在无治理模式下运行: {e}")

        # 初始化知识推送引擎（可选，不影响 pipeline 正常运行）
        self.knowledge_pusher = None
        if KNOWLEDGE_PUSHER_AVAILABLE:
            try:
                knowledge_dir = self.project_dir / "knowledge"
                tech_stack = {
                    "frontend": self.config_manager.config.frontend or "",
                    "backend": self.config_manager.config.backend or "",
                    "database": self.config_manager.config.database or "",
                }
                self.knowledge_pusher = KnowledgePusher(
                    knowledge_dir=knowledge_dir,
                    tech_stack=tech_stack,
                    project_description=self.config_manager.config.description or "",
                )
            except Exception as e:
                self.logger.warning(f"知识推送引擎初始化失败，pipeline 将在无推送模式下运行: {e}")

        # Initialize hook manager (optional, doesn't affect pipeline)
        self.hook_manager = None
        if HOOKS_AVAILABLE:
            try:
                import yaml

                config_path = self.project_dir / "super-dev.yaml"
                yaml_config = {}
                if config_path.exists():
                    with open(config_path, encoding="utf-8") as f:
                        yaml_config = yaml.safe_load(f) or {}
                self.hook_manager = HookManager.from_yaml_config(yaml_config, self.project_dir)
            except Exception as e:
                self.logger.warning(f"Hook 管理器初始化失败: {e}")

        # Initialize compact engine (optional)
        self.compact_engine = None
        if COMPACT_AVAILABLE:
            try:
                self.compact_engine = CompactEngine(self.project_dir)
            except Exception as e:
                self.logger.warning(f"上下文压缩引擎初始化失败: {e}")

        # Initialize memory system (optional)
        self.memory_store = None
        self.memory_extractor = None
        self.memory_consolidator = None
        if MEMORY_AVAILABLE:
            try:
                memory_dir = self.project_dir / ".super-dev" / "memory"
                self.memory_store = MemoryStore(memory_dir)
                self.memory_extractor = MemoryExtractor(self.memory_store)
                self.memory_consolidator = MemoryConsolidator(memory_dir)
            except Exception as e:
                self.logger.warning(f"记忆系统初始化失败: {e}")

        # Initialize pipeline cost tracker (optional)
        self.cost_tracker = None
        if COST_TRACKER_AVAILABLE:
            try:
                self.cost_tracker = PipelineCostTracker()
            except Exception as e:
                self.logger.warning(f"成本追踪器初始化失败: {e}")

        # Initialize plan executor (optional)
        self.plan_executor = None
        if PLAN_EXECUTOR_AVAILABLE:
            try:
                self.plan_executor = PlanExecutor(self.project_dir)
            except Exception as e:
                self.logger.warning(f'Plan-Execute 引擎初始化失败: {e}')

        # Initialize overseer (optional)
        self.overseer = None
        if OVERSEER_AVAILABLE:
            try:
                config = self.config_manager.config
                if getattr(config, 'overseer_enabled', False):
                    self.overseer = Overseer(
                        project_dir=self.project_dir,
                        project_name=getattr(config, 'name', ''),
                        quality_threshold=getattr(config, 'quality_gate', 80),
                        halt_on_critical=getattr(config, 'overseer_halt_on_critical', True),
                    )
            except Exception as e:
                self.logger.warning(f'Overseer 初始化失败: {e}')

        self.logger.info("工作流引擎初始化完成", extra={"project_dir": str(self.project_dir)})

    def _emit_state_event(
        self, event: str, phase: str = "", **kwargs: Any
    ) -> None:
        """Emit a pipeline state change event to interested subsystems.

        Events: ``phase_started``, ``phase_completed``, ``phase_failed``,
        ``pipeline_completed``.  Each subsystem handles errors internally
        so a failing listener never breaks the pipeline.
        """
        if event == "phase_started" and SESSION_BRIEF_AVAILABLE:
            try:
                SessionBrief.update_section(
                    self.project_dir,
                    "Current State",
                    f"Running phase: {phase}",
                )
            except Exception:
                pass

        if event == "phase_completed" and self.memory_extractor:
            try:
                ctx = kwargs.get("context")
                if ctx is not None:
                    phase_context = {
                        "user_input": ctx.user_input,
                        "research_data": ctx.research_data,
                        "documents": ctx.documents,
                        "quality_reports": ctx.quality_reports,
                        "output": kwargs.get("output"),
                    }
                    if self.memory_extractor.should_extract(phase, phase_context):
                        self.memory_extractor.extract_from_phase(phase, phase_context)
            except Exception:
                pass

        # Overseer checkpoint on phase completion
        if event == 'phase_completed' and self.overseer:
            try:
                quality_score = kwargs.get('quality_score', 0.0)
                self.overseer.checkpoint_phase(
                    phase=phase,
                    quality_score=quality_score,
                    actual_output=kwargs.get('output'),
                )
                if self.overseer.should_halt():
                    self.logger.warning(
                        f'Overseer 暂停流水线: {self.overseer.get_report().halt_reason}'
                    )
            except Exception:
                pass

        if event == "phase_failed":
            # Persist error state so resume can report the failure reason
            try:
                err_dir = self.project_dir / ".super-dev" / "errors"
                err_dir.mkdir(parents=True, exist_ok=True)
                err_file = err_dir / f"{phase}.json"
                err_file.write_text(
                    json.dumps(
                        {"phase": phase, "error": kwargs.get("error", ""), "event": event},
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n",
                    encoding="utf-8",
                )
            except Exception:
                pass

        if event == "pipeline_completed":
            # Trigger memory consolidation
            if self.memory_consolidator:
                try:
                    self.memory_consolidator.increment_session_count()
                    if self.memory_consolidator.should_consolidate():
                        self.memory_consolidator.consolidate()
                except Exception:
                    pass
            # Save cost metrics
            if self.cost_tracker:
                try:
                    self.cost_tracker.save(self.project_dir)
                except Exception:
                    pass

    def _emit_pipeline_state(
        self,
        current_phase: str,
        phases: list[Phase],
        results: dict[Phase, PhaseResult],
    ) -> None:
        """Write current pipeline state to .super-dev/pipeline-state.json.

        This file is read by ``super-dev status`` and can be referenced in
        SKILL.md so the host knows which phase the pipeline is in.
        """
        try:
            all_names = [p.value for p in phases]
            completed = [p.value for p in phases if p in results and results[p].success]
            phase_idx = all_names.index(current_phase) if current_phase in all_names else 0
            remaining = all_names[phase_idx:]

            state = {
                "current_phase": current_phase,
                "phase_index": phase_idx + 1,
                "total_phases": len(phases),
                "phases_completed": completed,
                "phases_remaining": remaining,
                "started_at": self._pipeline_started_at,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }

            state_dir = self.project_dir / ".super-dev"
            state_dir.mkdir(parents=True, exist_ok=True)
            state_path = state_dir / "pipeline-state.json"
            state_path.write_text(
                json.dumps(state, indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass  # Never break the pipeline for state tracking

    def _register_default_handlers(self) -> None:
        """注册默认阶段处理器"""
        self._phase_handlers[Phase.DISCOVERY] = self._phase_discovery
        self._phase_handlers[Phase.INTELLIGENCE] = self._phase_intelligence
        self._phase_handlers[Phase.DRAFTING] = self._phase_drafting
        self._phase_handlers[Phase.REDTEAM] = self._phase_redteam
        self._phase_handlers[Phase.QA] = self._phase_qa
        self._phase_handlers[Phase.DELIVERY] = self._phase_delivery
        self._phase_handlers[Phase.DEPLOYMENT] = self._phase_deployment

    def register_phase_handler(self, phase: Phase, handler: Callable) -> None:
        self._phase_handlers[phase] = handler

    def _save_checkpoint(self, phase: Phase, result: PhaseResult, context: WorkflowContext) -> None:
        """保存阶段检查点"""
        self._checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint = {
            "phase": phase.value,
            "success": result.success,
            "duration": result.duration,
            "quality_score": result.quality_score,
            "errors": [str(e) for e in result.errors],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "project": self.config_manager.config.name,
        }
        path = self._checkpoint_dir / f"{phase.value}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2, ensure_ascii=False)

    def _load_checkpoint(self, phase: Phase) -> dict | None:
        """加载阶段检查点"""
        path = self._checkpoint_dir / f"{phase.value}.json"
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or "phase" not in data or "success" not in data:
                self.logger.warning(f"检查点数据不完整，忽略: {path}")
                return None
            return data
        except (json.JSONDecodeError, OSError) as e:
            self.logger.warning(f"检查点文件损坏，忽略: {path}, 错误: {e}")
            return None

    def _clear_checkpoints(self) -> None:
        """清除所有检查点"""
        if self._checkpoint_dir.exists():
            for f in self._checkpoint_dir.glob("*.json"):
                f.unlink(missing_ok=True)

    def _restore_phase_outputs(self, phase: Phase, context: WorkflowContext) -> None:
        """恢复已完成阶段的输出数据到 context，供后续阶段使用。

        在 --resume 模式下，已完成的阶段会被跳过，但后续阶段可能依赖这些阶段
        写入 context 的数据（如 knowledge_bundle、documents、quality_reports）。
        此方法从 output/ 目录重新加载关键数据。
        """
        output_dir = context.project_dir / "output"
        if not output_dir.exists():
            return

        config_obj = getattr(context, "config", None)
        config_data = getattr(config_obj, "config", config_obj)
        name = str(
            context.user_input.get(
                "name", getattr(config_data, "name", None) or context.project_dir.name
            )
        )

        if phase == Phase.DISCOVERY:
            # 恢复知识缓存 bundle
            cache_dir = output_dir / "knowledge-cache"
            if cache_dir.exists():
                for bundle_file in cache_dir.glob("*-knowledge-bundle.json"):
                    try:
                        data = json.loads(bundle_file.read_text(encoding="utf-8"))
                        context.research_data["knowledge_bundle"] = data
                        # 恢复 enriched_description（discovery 阶段会写入此字段）
                        enriched = data.get("enriched_requirement")
                        if enriched:
                            context.user_input["enriched_description"] = enriched
                        # 恢复知识增强标记
                        context.user_input["knowledge_enhanced"] = bool(
                            data.get("local_knowledge") or data.get("local_items")
                        )
                        context.user_input["web_research"] = bool(
                            data.get("web_knowledge") or data.get("web_items")
                        )
                        break
                    except Exception:
                        pass

        elif phase == Phase.INTELLIGENCE:
            # intelligence 阶段的数据是联网检索结果，无持久化文件
            # 确保 research_data 中有 intelligence 键，避免后续访问出错
            if "intelligence" not in context.research_data:
                context.research_data["intelligence"] = {
                    "trends": [],
                    "competitors": [],
                    "best_practices": [],
                }

        elif phase == Phase.DRAFTING:
            # 恢复三大文档（PRD / architecture / UIUX）
            for doc_type in ("prd", "architecture", "uiux"):
                doc_path = output_dir / f"{name}-{doc_type}.md"
                if doc_path.exists():
                    context.documents[doc_type] = doc_path.read_text(encoding="utf-8")

        elif phase == Phase.REDTEAM:
            # 恢复红队报告元数据（QA 阶段依赖 context.quality_reports["redteam"]）
            redteam_json_path = output_dir / f"{name}-redteam.json"
            if redteam_json_path.exists():
                try:
                    context.quality_reports["redteam"] = json.loads(
                        redteam_json_path.read_text(encoding="utf-8")
                    )
                except Exception:
                    pass

    async def run(
        self,
        phases: list[Phase] | None = None,
        context: WorkflowContext | None = None,
        stop_requested: Callable[[], bool] | None = None,
        resume: bool = False,
        phase_timeout: int = 600,
    ) -> dict[Phase, PhaseResult]:
        self._pipeline_started_at = datetime.now(timezone.utc).isoformat()
        if context is None:
            context = WorkflowContext(project_dir=self.project_dir, config=self.config_manager)
        self._seed_context_user_input(context)

        if phases is None:
            phases = self._get_phases_from_config()

        # 启动治理层
        project_name = context.user_input.get("name", self.config_manager.config.name or "project")
        if self.governance:
            try:
                self.governance.start_governance(project_name)
            except Exception as e:
                self.logger.warning(f"治理层启动失败，继续无治理模式: {e}")

        results = {}
        if not resume:
            self._clear_checkpoints()

        # Cost tracker: mark pipeline start
        if self.cost_tracker:
            try:
                self.cost_tracker.start_pipeline()
            except Exception as e:
                self.logger.warning(f"成本追踪器 start_pipeline 失败: {e}")

        self._print_workflow_start(phases)

        for phase in phases:
            if stop_requested and stop_requested():
                self.logger.warning(
                    f"工作流收到停止请求，在阶段 {phase.value} 开始前终止",
                    extra={"phase": phase.value},
                )
                break

            # 检查点恢复：跳过已成功完成的阶段
            if resume:
                checkpoint = self._load_checkpoint(phase)
                if (
                    checkpoint
                    and checkpoint.get("success")
                    and checkpoint.get("project") == self.config_manager.config.name
                ):
                    self.logger.info(
                        f"恢复模式：跳过已完成阶段 {phase.value}", extra={"phase": phase.value}
                    )
                    results[phase] = PhaseResult(
                        phase=phase,
                        success=True,
                        duration=checkpoint.get("duration", 0.0),
                        quality_score=checkpoint.get("quality_score", 0.0),
                    )
                    if self.console:
                        self.console.print(
                            f"[blue]↪[/blue] {phase.value}: "
                            f"已恢复 (跳过，上次耗时 {checkpoint.get('duration', 0):.1f}s)"
                        )
                    # 恢复已完成阶段的输出数据到 context，供后续阶段使用
                    try:
                        self._restore_phase_outputs(phase, context)
                    except Exception as e:
                        self.logger.warning(f"恢复阶段 {phase.value} 输出数据失败: {e}")
                    continue

            # 输出阶段标识
            phase_idx = phases.index(phase) + 1
            self._print_phase_start(phase, phase_idx, len(phases))

            # Emit pipeline state for host awareness
            self._emit_pipeline_state(phase.value, phases, results)

            # Cost tracker: mark phase start
            if self.cost_tracker:
                try:
                    self.cost_tracker.start_phase(phase.value)
                except Exception as e:
                    self.logger.warning(f"成本追踪器 start_phase({phase.value}) 失败: {e}")

            # 治理层：进入阶段
            if self.governance:
                try:
                    self.governance.enter_phase(phase.value)
                except Exception as e:
                    self.logger.warning(f"治理层 enter_phase({phase.value}) 失败: {e}")

            # Overseer halt check
            if self.overseer and self.overseer.should_halt():
                halt_reason = self.overseer.get_report().halt_reason
                self.logger.warning(
                    f'Overseer 要求暂停，在阶段 {phase.value} 开始前终止: {halt_reason}'
                )
                results[phase] = PhaseResult(
                    phase=phase,
                    success=False,
                    duration=0.0,
                    errors=[halt_reason],
                )
                self._save_checkpoint(phase, results[phase], context)
                self._emit_state_event("phase_failed", phase.value, error=halt_reason)
                break

            try:
                # Hook: PrePhase
                if self.hook_manager:
                    try:
                        hook_results = self.hook_manager.execute(
                            HookEvent.PRE_PHASE,
                            context={
                                "phase": phase.value,
                                "name": context.user_input.get("name", ""),
                            },
                            phase=phase.value,
                        )
                        if self.hook_manager.has_blocking_result(hook_results):
                            raise PhaseExecutionError(
                                phase=phase.value,
                                message=f"PrePhase hook blocked execution for {phase.value}",
                            )
                    except PhaseExecutionError:
                        raise
                    except Exception as e:
                        self.logger.warning(f"PrePhase hook 执行失败: {e}")

                # Inject compact context from previous phases
                if self.compact_engine and results:
                    try:
                        previous_phases = [p.value for p in results if results[p].success]
                        compact_context = self.compact_engine.build_context_for_phase(
                            phase.value, previous_phases
                        )
                        if compact_context:
                            context.metadata["compact_context"] = compact_context
                    except Exception as e:
                        self.logger.warning(f"上下文压缩注入失败: {e}")

                result = await self._run_phase(phase, context, phase_timeout=phase_timeout)
                results[phase] = result

                # Compact: save phase summary
                if self.compact_engine and result.success:
                    try:
                        summary = self.compact_engine.compact_phase_output(
                            phase.value,
                            {
                                "user_input": context.user_input,
                                "research_data": context.research_data,
                                "documents": context.documents,
                                "quality_reports": context.quality_reports,
                                "output": result.output,
                            },
                        )
                        self.compact_engine.save_compact(summary)
                    except Exception as e:
                        self.logger.warning(f"阶段压缩保存失败: {e}")

                # Memory: extract memories from phase
                if self.memory_extractor and result.success:
                    try:
                        phase_context = {
                            "user_input": context.user_input,
                            "research_data": context.research_data,
                            "documents": context.documents,
                            "quality_reports": context.quality_reports,
                            "output": result.output,
                        }
                        if self.memory_extractor.should_extract(phase.value, phase_context):
                            entries = self.memory_extractor.extract_from_phase(
                                phase.value, phase_context
                            )
                            if entries:
                                self.logger.info(f"记忆提取 [{phase.value}]: {len(entries)} 条")
                    except Exception as e:
                        self.logger.warning(f"记忆提取失败: {e}")

                # Hook: PostPhase
                if self.hook_manager:
                    try:
                        self.hook_manager.execute(
                            HookEvent.POST_PHASE,
                            context={
                                "phase": phase.value,
                                "success": str(result.success),
                                "quality_score": str(result.quality_score),
                            },
                            phase=phase.value,
                        )
                    except Exception as e:
                        self.logger.warning(f"PostPhase hook 执行失败: {e}")

                # 治理层：退出阶段
                if self.governance:
                    try:
                        self.governance.exit_phase(
                            phase.value,
                            context={
                                "success": result.success,
                                "duration": result.duration,
                                "quality_score": result.quality_score,
                                "project_dir": str(self.project_dir),
                                "name": context.user_input.get("name", self.project_dir.name),
                                "output_dir": str(self.project_dir / "output"),
                            },
                        )
                    except Exception as e:
                        self.logger.warning(f"治理层 exit_phase({phase.value}) 失败: {e}")

                # 保存检查点
                if result.success:
                    self._save_checkpoint(phase, result, context)

                self._emit_state_event(
                    "phase_completed",
                    phase.value,
                    output=result.output,
                    context=context,
                    quality_score=result.quality_score,
                )

                try:
                    review_phases = set(
                        getattr(self.config_manager.config, "codex_review_phases", []) or []
                    )
                    if (
                        self.plan_executor
                        and self.overseer
                        and getattr(self.config_manager.config, "codex_review_enabled", False)
                        and phase.value in review_phases
                    ):
                        from types import SimpleNamespace

                        review_step = SimpleNamespace(
                            id=f"phase-{phase.value}",
                            label=f"{phase.value} phase review",
                            phase=phase.value,
                            description=f"Review completed output for phase {phase.value}.",
                        )
                        review_context = json.dumps(
                            {
                                "phase": phase.value,
                                "quality_score": result.quality_score,
                                "output": result.output,
                            },
                            ensure_ascii=False,
                            default=str,
                            indent=2,
                        )
                        codex_review = self.plan_executor.request_codex_review(
                            review_step, review_context
                        )
                        if not bool(codex_review.get("passed", True)):
                            parsed_review = codex_review.get("parsed")
                            issue_text = ""
                            if isinstance(parsed_review, dict):
                                parsed_issues = parsed_review.get("issues")
                                if isinstance(parsed_issues, list) and parsed_issues:
                                    issue_text = "; ".join(str(item) for item in parsed_issues)
                                else:
                                    issue_text = str(parsed_review.get("summary", "")).strip()
                            self.overseer.checkpoint_phase(
                                phase=phase.value,
                                quality_score=result.quality_score,
                                actual_output=result.output if isinstance(result.output, dict) else None,
                                codex_reviews=[
                                    {
                                        "severity": codex_review.get("severity", "unknown"),
                                        "issue": (
                                            issue_text
                                            or str(codex_review.get("error", "")).strip()
                                            or "Codex review did not pass."
                                        ),
                                    }
                                ],
                            )
                except Exception:
                    pass

                # Update pipeline state after phase completion
                self._emit_pipeline_state(phase.value, phases, results)

                # Cost tracker: mark phase end
                if self.cost_tracker:
                    try:
                        self.cost_tracker.end_phase(phase.value)
                    except Exception as e:
                        self.logger.warning(f"成本追踪器 end_phase({phase.value}) 失败: {e}")

                # 红队审查必须通过，避免风险进入后续阶段
                if phase == Phase.REDTEAM and isinstance(result.output, dict):
                    if not bool(result.output.get("passed", True)):
                        reasons = result.output.get("blocking_reasons") or ["红队审查未通过"]
                        reason_text = "; ".join(str(r) for r in reasons)
                        quality_error = QualityGateError(
                            score=float(result.output.get("score", result.quality_score)),
                            threshold=float(result.output.get("pass_threshold", 70)),
                            details={"phase": phase.value, "reasons": reasons},
                        )
                        result.success = False
                        result.errors.append(reason_text)
                        raise quality_error

                # 质量门禁只在 QA 阶段执行，避免前置阶段被全局阈值误杀
                if (
                    phase == Phase.QA
                    and result.quality_score < self.config_manager.config.quality_gate
                ):
                    self._print_quality_gate_failed(phase, result)
                    quality_error = QualityGateError(
                        score=result.quality_score,
                        threshold=self.config_manager.config.quality_gate,
                        details={"phase": phase.value},
                    )
                    result.success = False
                    result.errors.append(str(quality_error))
                    raise quality_error

                self._print_phase_complete(phase, result)

            except QualityGateError as e:
                self.logger.error(
                    f"工作流在阶段 {phase.value} 因质量门禁终止",
                    extra={"error": str(e), "phase": phase.value},
                )
                if phase in results:
                    results[phase].success = False
                    if str(e) not in results[phase].errors:
                        results[phase].errors.append(str(e))
                self._save_cost_tracker()  # 异常中断也保存成本
                break

            except PhaseExecutionError as e:
                # 非关键阶段可以跳过，不中断整个流水线
                skippable_phases = {Phase.INTELLIGENCE}
                if phase in skippable_phases:
                    self.logger.warning(
                        f"阶段 {phase.value} 执行失败但可跳过: {e}",
                        extra={"error": str(e), "phase": phase.value},
                    )
                    results[phase] = PhaseResult(
                        phase=phase,
                        success=True,
                        duration=0.0,
                        errors=[str(e)],
                        quality_score=0.0,
                    )
                    if self.console:
                        try:
                            self.console.print(
                                f"[yellow]⚠[/yellow] {phase.value}: " f"执行失败但已跳过 ({e})"
                            )
                        except Exception:
                            pass
                    continue
                self.logger.error(
                    f"工作流在阶段 {phase.value} 终止",
                    extra={"error": str(e), "phase": phase.value},
                )
                results[phase] = PhaseResult(
                    phase=phase, success=False, duration=0.0, errors=[str(e)]
                )
                break

        overseer_report = None

        # Finalize overseer report
        if self.overseer:
            try:
                overseer_report = self.overseer.finalize()
                self.logger.info(
                    f'Overseer 报告: {overseer_report.overall_verdict.value}, '
                    f'偏差数: {overseer_report.total_deviations}'
                )
            except Exception as e:
                self.logger.warning(f'Overseer 最终报告生成失败: {e}')

        try:
            if overseer_report is not None:
                output_dir = self.project_dir / "output"
                output_dir.mkdir(parents=True, exist_ok=True)
                name = str(
                    context.user_input.get(
                        "name", self.config_manager.config.name or self.project_dir.name
                    )
                )
                (output_dir / f"{name}-overseer.md").write_text(
                    overseer_report.to_markdown(),
                    encoding="utf-8",
                )
        except Exception:
            pass

        # Session Brief: update status
        if SESSION_BRIEF_AVAILABLE:
            try:
                SessionBrief.update_section(
                    self.project_dir,
                    "Current State",
                    f"Pipeline completed. Phases: {', '.join(p.value for p in results)}. "
                    f"Success: {sum(1 for r in results.values() if r.success)}/{len(results)}.",
                )
            except Exception as e:
                self.logger.warning(f"Session Brief 更新失败: {e}")

        # Memory: trigger dream consolidation if conditions met
        if self.memory_consolidator:
            try:
                self.memory_consolidator.increment_session_count()
                if self.memory_consolidator.should_consolidate():
                    consolidation_result = self.memory_consolidator.consolidate()
                    if consolidation_result.errors:
                        self.logger.warning(f"记忆整合有错误: {consolidation_result.errors}")
                    else:
                        self.logger.info(
                            f"记忆整合完成: 删除 {consolidation_result.files_deleted}, "
                            f"矛盾解决 {consolidation_result.contradictions_resolved}"
                        )
            except Exception as e:
                self.logger.warning(f"记忆整合失败: {e}")

        self._print_workflow_complete(results)
        self._save_report(results)

        # Cost tracker: save in finally-equivalent position
        # 即使 pipeline 因异常中断，成本数据也会被保存
        self._save_cost_tracker()

        # 治理层：生成治理报告
        governance_report = None
        if self.governance:
            try:
                governance_report = self.governance.finish_governance()
                self.logger.info(
                    "治理报告已生成",
                    extra={
                        "governance_passed": getattr(governance_report, "passed", None),
                        "governance_score": getattr(governance_report, "quality_score", None),
                    },
                )
            except Exception as e:
                self.logger.warning(f"治理报告生成失败: {e}")

        return results

    def _save_cost_tracker(self) -> None:
        """保存成本追踪数据（在正常和异常路径都调用）。"""
        if self.cost_tracker:
            try:
                self.cost_tracker.save(self.project_dir)
            except Exception as e:
                self.logger.warning(f"成本追踪器保存失败: {e}")

    def _get_phases_from_config(self) -> list[Phase]:
        config_phases = self.config_manager.config.phases
        phases = []
        phase_map = {
            "discovery": Phase.DISCOVERY,
            "intelligence": Phase.INTELLIGENCE,
            "drafting": Phase.DRAFTING,
            "redteam": Phase.REDTEAM,
            "qa": Phase.QA,
            "delivery": Phase.DELIVERY,
            "deployment": Phase.DEPLOYMENT,
        }
        for p in config_phases:
            if p in phase_map:
                phases.append(phase_map[p])
        return phases

    async def _run_phase(
        self, phase: Phase, context: WorkflowContext, phase_timeout: int = 600
    ) -> PhaseResult:
        start_time = datetime.now()
        phase_name = phase.value.upper()

        self.logger.info(f"开始执行阶段: {phase_name}", extra={"phase": phase_name})

        # 知识推送：每个阶段开始时推送与该阶段相关的知识约束
        # 优先使用三层渐进式加载（push_layered），回退到传统 push
        if self.knowledge_pusher:
            try:
                description = context.user_input.get(
                    "enriched_description",
                    context.user_input.get("description", ""),
                )
                # 三层渐进式加载：L1 索引 + L2 详情 + L3 引用
                layered_push = self.knowledge_pusher.push_layered(
                    phase.value,
                    description=description,
                    token_budget=4000,  # 每阶段最多 4K tokens 的知识
                )
                context.metadata[f"knowledge_push_layered_{phase.value}"] = layered_push.to_dict()
                if layered_push.l1_index:
                    self.logger.info(
                        f"分层知识推送 [{phase_name}]: "
                        f"L1={len(layered_push.l1_index)} 索引, "
                        f"L2={len(layered_push.l2_details)} 详情, "
                        f"L3={len(layered_push.l3_references)} 引用, "
                        f"tokens={layered_push.l1_tokens_used + layered_push.l2_tokens_used}"
                        f"/{layered_push.total_token_budget}",
                        extra={"phase": phase_name},
                    )

                # 同时保留传统 push 结果以确保向后兼容
                knowledge_push = self.knowledge_pusher.push(
                    phase.value, project_description=description
                )
                context.metadata[f"knowledge_push_{phase.value}"] = knowledge_push.to_dict()
                if knowledge_push.files:
                    self.logger.info(
                        f"知识推送 [{phase_name}]: {len(knowledge_push.files)} 文件, "
                        f"{len(knowledge_push.constraints)} 约束, "
                        f"{len(knowledge_push.antipatterns)} 反模式",
                        extra={"phase": phase_name},
                    )
            except Exception as e:
                self.logger.warning(f"知识推送失败 [{phase_name}]: {e}")

        try:
            handler = self._phase_handlers.get(phase)
            if handler is None:
                raise PhaseExecutionError(
                    phase=phase_name,
                    message=f"No handler registered for phase: {phase}",
                    details={"available_phases": list(self._phase_handlers.keys())},
                )

            try:
                output = await asyncio.wait_for(
                    self._execute_handler_async(handler, context),
                    timeout=phase_timeout,
                )
            except asyncio.TimeoutError:
                duration = (datetime.now() - start_time).total_seconds()
                raise PhaseExecutionError(
                    phase=phase_name,
                    message=f"阶段 {phase_name} 超时（限制 {phase_timeout} 秒）",
                    details={"timeout": phase_timeout, "duration": duration},
                )
            duration = (datetime.now() - start_time).total_seconds()

            # 兼容自定义处理器直接返回 PhaseResult 的场景
            if isinstance(output, PhaseResult):
                result = output
                if result.phase != phase:
                    result.phase = phase
                if result.duration <= 0:
                    result.duration = duration
                if result.quality_score <= 0:
                    result.quality_score = self._calculate_quality_score(phase, context)
                return result

            quality_score = self._calculate_quality_score(phase, context)

            self.logger.info(
                f"阶段执行成功: {phase_name}",
                extra={"phase": phase_name, "duration": duration, "quality_score": quality_score},
            )

            return PhaseResult(
                phase=phase,
                success=True,
                duration=duration,
                output=output,
                quality_score=quality_score,
            )

        except PhaseExecutionError:
            raise
        except QualityGateError:
            raise
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_details = {
                "error_type": type(e).__name__,
                "error_message": str(e),
                "traceback": traceback.format_exc(),
                "phase": phase_name,
                "duration": duration,
            }
            self.logger.error(f"阶段执行失败: {phase_name}", extra=error_details)
            raise PhaseExecutionError(
                phase=phase_name, message=f"Phase execution failed: {str(e)}", details=error_details
            ) from e

    async def _execute_handler_async(self, handler: Callable, context: WorkflowContext) -> Any:
        if asyncio.iscoroutinefunction(handler):
            return await handler(context)
        else:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, handler, context)

    _execute_handler = _execute_handler_async

    def _calculate_quality_score(self, phase: Phase, context: WorkflowContext) -> float:
        """使用真实质量评分引擎计算分数"""
        try:
            from .quality import QualityScorer

            configured_name = "project"
            config_obj = getattr(context, "config", None)
            if config_obj is not None:
                # 兼容 WorkflowContext.config 为 ConfigManager 或 ProjectConfig 两种形态
                manager_cfg = getattr(config_obj, "config", None)
                if manager_cfg is not None:
                    configured_name = getattr(manager_cfg, "name", None) or configured_name
                else:
                    configured_name = getattr(config_obj, "name", None) or configured_name

            name = str(context.user_input.get("name", configured_name))
            scorer = QualityScorer(project_dir=self.project_dir, name=name)
            score = scorer.score_phase(
                phase_name=phase.value,
                context_data=context.user_input,
            )
            return float(score)
        except Exception as e:
            self.logger.warning(f"质量评分失败，使用默认值: {e}")
            return 75.0

    # ==================== 阶段处理器（真实实现）====================

    async def _phase_discovery(self, context: WorkflowContext) -> Any:
        """
        第 0 阶段：需求增强
        - 解析用户输入的自然语言需求
        - 注入领域知识库
        - 联网检索补充背景信息（可选）
        """
        from ..creators.requirement_parser import RequirementParser
        from .knowledge import KnowledgeAugmenter

        user_input = context.user_input
        description = user_input.get("enriched_description", user_input.get("description", ""))
        domain = user_input.get("domain", "")
        offline = user_input.get("offline", False)
        config_obj = getattr(context, "config", None)
        config_data = getattr(config_obj, "config", config_obj)
        project_name = str(user_input.get("name", getattr(config_data, "name", "") or "project"))
        allowed_domains = list(getattr(config_data, "knowledge_allowed_domains", []) or [])
        cache_ttl_seconds = getattr(config_data, "knowledge_cache_ttl_seconds", None)

        # 1. 解析结构化需求
        parser = RequirementParser()
        requirements = parser.parse_requirements(description)
        scenario = parser.detect_scenario(self.project_dir)
        request_mode = parser.detect_request_mode(description)
        context.user_input["scenario"] = scenario
        context.user_input["request_mode"] = request_mode
        context.user_input["requirements"] = requirements

        # 2. 需求知识增强（本地知识库 + 联网检索）
        augmenter = None
        try:
            augmenter = KnowledgeAugmenter(
                project_dir=self.project_dir,
                web_enabled=not offline,
                allowed_web_domains=allowed_domains,
                cache_ttl_seconds=cache_ttl_seconds,
            )
            output_dir = self.project_dir / "output"
            knowledge_bundle = augmenter.load_cached_bundle(
                output_dir=output_dir,
                project_name=project_name,
                requirement=description,
                domain=domain,
            )
            if knowledge_bundle is None:
                knowledge_bundle = augmenter.augment(
                    requirement=description,
                    domain=domain,
                    max_local_results=12,
                    max_web_results=8,
                )
                augmenter.save_bundle(
                    bundle=knowledge_bundle,
                    output_dir=output_dir,
                    project_name=project_name,
                    requirement=description,
                    domain=domain,
                )
            context.research_data["knowledge_bundle"] = knowledge_bundle
            context.user_input["knowledge_enhanced"] = bool(knowledge_bundle.get("local_knowledge"))
            context.user_input["web_research"] = bool(knowledge_bundle.get("web_knowledge"))
            context.user_input["enriched_description"] = knowledge_bundle.get(
                "enriched_requirement", description
            )
        except Exception as e:
            self.logger.warning(f"需求知识增强跳过: {e}")
            context.user_input["knowledge_enhanced"] = False
            context.user_input["web_research"] = False

        # 断点修复3: 将 augmenter 的知识引用追踪同步到 governance 层
        try:
            if (
                self.governance
                and augmenter is not None
                and hasattr(augmenter, "_tracker")
                and augmenter._tracker
            ):
                for ref in augmenter._tracker.references:
                    try:
                        self.governance.track_knowledge(
                            ref.knowledge_file,
                            usage_type=ref.usage_type,
                            relevance=ref.relevance_score,
                            excerpt=getattr(ref, "excerpt", ""),
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        return {
            "status": "discovery_complete",
            "scenario": scenario,
            "request_mode": request_mode,
            "requirements_count": len(requirements),
            "knowledge_enhanced": context.user_input.get("knowledge_enhanced"),
            "web_research": context.user_input.get("web_research"),
        }

    async def _phase_intelligence(self, context: WorkflowContext) -> Any:
        """
        第 0.5 阶段：市场情报
        - 基于需求进行深度联网检索
        - 竞品分析
        - 技术趋势调研
        """
        description = context.user_input.get(
            "enriched_description",
            context.user_input.get("description", ""),
        )
        offline = context.user_input.get("offline", False)

        intelligence: dict[str, list[dict[str, str]]] = {
            "trends": [],
            "competitors": [],
            "best_practices": [],
        }

        if not offline and description:
            try:
                # 技术趋势检索
                trend_results = await self._web_search(f"{description} best practices 2024 2025")
                if trend_results:
                    intelligence["trends"] = trend_results[:3]

                # 竞品检索
                competitor_results = await self._web_search(f"{description} top alternatives tools")
                if competitor_results:
                    intelligence["competitors"] = competitor_results[:3]

            except Exception as e:
                self.logger.info(f"市场情报检索跳过: {e}")

        context.research_data["intelligence"] = intelligence

        return {
            "status": "intelligence_complete",
            "trends_count": len(intelligence["trends"]),
            "competitors_count": len(intelligence["competitors"]),
        }

    async def _phase_drafting(self, context: WorkflowContext) -> Any:
        """
        第 1 阶段：专家协作生成文档
        - PM 专家：生成 PRD
        - ARCHITECT 专家：生成架构文档
        - UI/UX 专家：生成 UI/UX 文档
        """
        from .experts import ExpertDispatcher

        user_input = context.user_input
        name = user_input.get("name", self.config_manager.config.name or "project")
        description = user_input.get("enriched_description", user_input.get("description", ""))
        platform = user_input.get("platform", "web")
        frontend = user_input.get("frontend", "react")
        backend = user_input.get("backend", "node")
        domain = user_input.get("domain", "")
        language_preferences = user_input.get("language_preferences", [])

        # 断点修复1: 从 discovery 阶段的知识包提取知识摘要传给文档生成器
        # 断点修复2: 将 intelligence 阶段的检索结果也纳入知识摘要
        knowledge_summary: dict = {}
        try:
            knowledge_bundle = context.research_data.get("knowledge_bundle", {})
            if knowledge_bundle:
                knowledge_summary = knowledge_bundle.get("research_summary", {})
                if not knowledge_summary:
                    knowledge_summary = {
                        "local_knowledge": knowledge_bundle.get("local_items", []),
                        "web_knowledge": knowledge_bundle.get("web_items", []),
                        "hard_constraints": knowledge_bundle.get("hard_constraints", []),
                    }
            intelligence_data = context.research_data.get("intelligence", {})
            if intelligence_data:
                knowledge_summary["web_intelligence"] = intelligence_data
        except Exception:
            pass

        # 断点修复: 从 KnowledgePusher 推送的 context.metadata 读取知识约束
        # 确保 _run_phase 中存入的 knowledge_push_* 数据被下游 DocumentGenerator 消费
        for phase_key in [
            "knowledge_push_drafting",
            "knowledge_push_docs",
            "knowledge_push_research",
            "knowledge_push_intelligence",
            "knowledge_push_discovery",
        ]:
            push_data = context.metadata.get(phase_key)
            if push_data and isinstance(push_data, dict):
                if "constraints" in push_data:
                    knowledge_summary.setdefault("pushed_constraints", []).extend(
                        push_data["constraints"]
                    )
                if "antipatterns" in push_data:
                    knowledge_summary.setdefault("pushed_antipatterns", []).extend(
                        push_data["antipatterns"]
                    )
                if "files" in push_data:
                    knowledge_summary.setdefault("pushed_knowledge_files", []).extend(
                        push_data["files"]
                    )

        dispatcher = ExpertDispatcher(self.project_dir)
        result = dispatcher.dispatch_document_generation(
            name=name,
            description=description,
            platform=platform,
            frontend=frontend,
            backend=backend,
            domain=domain,
            language_preferences=language_preferences,
            knowledge_summary=knowledge_summary,
        )

        # 保存生成的文档到 output/ 目录
        output_dir = self.project_dir / "output"
        output_dir.mkdir(exist_ok=True)

        saved_docs = []
        for expert_output in result.outputs:
            doc_path = output_dir / f"{name}-{expert_output.document_type}.md"
            doc_path.write_text(expert_output.content, encoding="utf-8")
            saved_docs.append(str(doc_path))
            context.documents[expert_output.document_type] = expert_output.content

        # 断点修复3: 调用 AIPromptGenerator 生成 ai-prompt.md（包含知识推送约束）
        try:
            from ..creators.prompt_generator import AIPromptGenerator

            prompt_gen = AIPromptGenerator(self.project_dir, name)
            prompt_content = prompt_gen.generate()
            prompt_path = output_dir / f"{name}-ai-prompt.md"
            prompt_path.write_text(prompt_content, encoding="utf-8")
            saved_docs.append(str(prompt_path))
            self.logger.info(f"AI 提示词已生成: {prompt_path}")
        except Exception as e:
            self.logger.warning(f"AI 提示词生成失败（非阻塞）: {e}")

        return {
            "status": "documents_generated",
            "documents": saved_docs,
            "team_score": result.total_score,
            "summary": result.summary,
        }

    async def _phase_redteam(self, context: WorkflowContext) -> Any:
        """
        第 5 阶段：红队审查
        - SECURITY 专家：安全审查（注入、XSS、CSRF 等）
        - 性能审查（N+1、缓存、分页）
        - 架构审查（可扩展性、可维护性）
        """
        from .experts import ExpertDispatcher

        user_input = context.user_input
        name = user_input.get("name", self.config_manager.config.name or "project")
        tech_stack = {
            "platform": user_input.get("platform", "web"),
            "frontend": user_input.get("frontend", "react"),
            "backend": user_input.get("backend", "node"),
            "domain": user_input.get("domain", ""),
        }

        dispatcher = ExpertDispatcher(self.project_dir)
        expert_output = dispatcher.dispatch_redteam_review(name=name, tech_stack=tech_stack)

        # 保存红队报告
        output_dir = self.project_dir / "output"
        output_dir.mkdir(exist_ok=True)
        redteam_path = output_dir / f"{name}-redteam.md"
        redteam_path.write_text(expert_output.content, encoding="utf-8")

        # 保存报告到上下文（供 QA 阶段使用）
        context.quality_reports["redteam"] = expert_output.metadata

        return {
            "status": "redteam_complete",
            "report_path": str(redteam_path),
            "score": expert_output.quality_score,
            "passed": expert_output.metadata.get("passed", False),
            "pass_threshold": expert_output.metadata.get("pass_threshold", 70),
            "blocking_reasons": expert_output.metadata.get("blocking_reasons", []),
            "critical_count": expert_output.metadata.get("critical_count", 0),
            "high_count": expert_output.metadata.get("high_count", 0),
            "issues": {
                "security": expert_output.metadata.get("security_issues", []),
                "performance": expert_output.metadata.get("performance_issues", []),
                "architecture": expert_output.metadata.get("architecture_issues", []),
            },
        }

    async def _phase_qa(self, context: WorkflowContext) -> Any:
        """
        第 6 阶段：质量门禁
        - QA 专家：多维度质量门禁检查
        - 场景化检查但统一 80+ 通过标准（支持手动覆盖）
        - 生成详细质量报告
        """
        from ..reviewers.redteam import RedTeamReviewer
        from .experts import ExpertDispatcher

        user_input = context.user_input
        name = user_input.get("name", self.config_manager.config.name or "project")
        tech_stack = {
            "platform": user_input.get("platform", "web"),
            "frontend": user_input.get("frontend", "react"),
            "backend": user_input.get("backend", "node"),
            "domain": user_input.get("domain", ""),
        }
        threshold_override = user_input.get("quality_threshold")
        host_compatibility_min_score_override = user_input.get(
            "host_compatibility_min_score",
            self.config_manager.config.host_compatibility_min_score,
        )
        host_compatibility_min_ready_hosts_override = user_input.get(
            "host_compatibility_min_ready_hosts",
            self.config_manager.config.host_compatibility_min_ready_hosts,
        )

        # 优先复用已有红队报告，避免重复执行
        redteam_report = context.quality_reports.get("redteam")
        if redteam_report is None:
            try:
                reviewer = RedTeamReviewer(
                    project_dir=self.project_dir,
                    name=name,
                    tech_stack=tech_stack,
                )
                redteam_report = reviewer.review()
            except Exception as e:
                self.logger.warning(f"红队报告加载失败，跳过: {e}")

        dispatcher = ExpertDispatcher(self.project_dir)
        expert_output = dispatcher.dispatch_quality_gate(
            name=name,
            tech_stack=tech_stack,
            redteam_report=redteam_report,
            threshold_override=threshold_override,
            host_compatibility_min_score_override=host_compatibility_min_score_override,
            host_compatibility_min_ready_hosts_override=host_compatibility_min_ready_hosts_override,
        )

        # 保存质量门禁报告
        output_dir = self.project_dir / "output"
        output_dir.mkdir(exist_ok=True)
        qg_path = output_dir / f"{name}-quality-gate.md"
        qg_path.write_text(expert_output.content, encoding="utf-8")
        ui_review_payload = expert_output.metadata.get("ui_review")
        if isinstance(ui_review_payload, dict):
            ui_review_md_path = output_dir / f"{name}-ui-review.md"
            ui_review_json_path = output_dir / f"{name}-ui-review.json"
            ui_alignment_md_path = output_dir / f"{name}-ui-contract-alignment.md"
            ui_alignment_json_path = output_dir / f"{name}-ui-contract-alignment.json"
            ui_review_json_path.write_text(
                json.dumps(ui_review_payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            ui_alignment_json_path.write_text(
                json.dumps(
                    ui_review_payload.get("alignment_summary", {}), ensure_ascii=False, indent=2
                ),
                encoding="utf-8",
            )
            if not ui_review_md_path.exists():
                lines = [
                    f"# {name} - UI 审查报告",
                    "",
                    f"- **总分**: {ui_review_payload.get('score', 0)}/100",
                    f"- **结论**: {'通过' if ui_review_payload.get('passed') else '需继续修正'}",
                    "",
                ]
                ui_review_md_path.write_text("\n".join(lines), encoding="utf-8")
            if not ui_alignment_md_path.exists():
                alignment = ui_review_payload.get("alignment_summary", {})
                lines = [
                    f"# {name} - UI 契约对齐报告",
                    "",
                ]
                for key, value in alignment.items():
                    if isinstance(value, dict):
                        lines.append(
                            f"- {value.get('label', key)}: {'ok' if value.get('passed') else 'gap'} | expected={value.get('expected', '-') or '-'} | observed={value.get('observed', '-') or '-'}"
                        )
                lines.append("")
                ui_alignment_md_path.write_text("\n".join(lines), encoding="utf-8")

        passed = expert_output.metadata.get("passed", False)
        if not passed:
            effective_threshold = (
                int(threshold_override)
                if threshold_override is not None
                else int(self.config_manager.config.quality_gate)
            )
            raise QualityGateError(
                score=expert_output.quality_score,
                threshold=effective_threshold,
                details={"phase": "qa", "report_path": str(qg_path)},
            )

        return {
            "status": "quality_gate_passed",
            "score": expert_output.quality_score,
            "scenario": expert_output.metadata.get("scenario"),
            "report_path": str(qg_path),
            "ui_review_path": (
                str(output_dir / f"{name}-ui-review.md")
                if isinstance(ui_review_payload, dict)
                else ""
            ),
            "ui_review_json_path": (
                str(output_dir / f"{name}-ui-review.json")
                if isinstance(ui_review_payload, dict)
                else ""
            ),
            "ui_alignment_path": (
                str(output_dir / f"{name}-ui-contract-alignment.md")
                if isinstance(ui_review_payload, dict)
                else ""
            ),
            "ui_alignment_json_path": (
                str(output_dir / f"{name}-ui-contract-alignment.json")
                if isinstance(ui_review_payload, dict)
                else ""
            ),
            "ui_review": ui_review_payload if isinstance(ui_review_payload, dict) else None,
        }

    async def _phase_delivery(self, context: WorkflowContext) -> Any:
        """
        第 7-8 阶段：代码审查指南 + AI 提示词生成
        - CODE 专家：代码审查指南
        - CODE 专家：AI 提示词（用户复制给 AI 即可开始开发）
        """
        from .experts import ExpertDispatcher

        user_input = context.user_input
        name = user_input.get("name", self.config_manager.config.name or "project")
        tech_stack = {
            "platform": user_input.get("platform", "web"),
            "frontend": user_input.get("frontend", "react"),
            "backend": user_input.get("backend", "node"),
            "domain": user_input.get("domain", ""),
        }

        dispatcher = ExpertDispatcher(self.project_dir)
        output_dir = self.project_dir / "output"
        output_dir.mkdir(exist_ok=True)
        saved = []
        step_errors: list[str] = []

        # 代码审查指南
        try:
            cr_output = dispatcher.dispatch_code_review(name=name, tech_stack=tech_stack)
            cr_path = output_dir / f"{name}-code-review.md"
            cr_path.write_text(cr_output.content, encoding="utf-8")
            saved.append(str(cr_path))
        except Exception as e:
            msg = f"代码审查指南生成失败: {e}"
            self.logger.error(msg)
            step_errors.append(msg)

        # AI 提示词
        try:
            ai_output = dispatcher.dispatch_ai_prompt(name=name)
            ai_path = output_dir / f"{name}-ai-prompt.md"
            ai_path.write_text(ai_output.content, encoding="utf-8")
            saved.append(str(ai_path))
        except Exception as e:
            msg = f"AI 提示词生成失败: {e}"
            self.logger.error(msg)
            step_errors.append(msg)

        if step_errors:
            raise PhaseExecutionError(
                phase=Phase.DELIVERY.value.upper(),
                message="Delivery phase failed",
                details={"errors": step_errors},
            )

        return {
            "status": "delivery_complete",
            "files": saved,
        }

    async def _phase_deployment(self, context: WorkflowContext) -> Any:
        """
        第 9-11 阶段：CI/CD 配置 + 数据库迁移 + 交付包
        - DEVOPS 专家：生成 5 大 CI/CD 平台配置
        - DBA 专家：生成 6 种 ORM 数据库迁移脚本
        - 交付收敛：生成 manifest/report/zip 交付包
        """
        from .. import __version__
        from ..deployers import DeliveryPackager
        from .experts import ExpertDispatcher

        user_input = context.user_input
        name = user_input.get("name", self.config_manager.config.name or "project")
        tech_stack = {
            "platform": user_input.get("platform", "web"),
            "frontend": user_input.get("frontend", "react"),
            "backend": user_input.get("backend", "node"),
            "domain": user_input.get("domain", ""),
        }
        cicd_platform = user_input.get("cicd", "github")
        orm = user_input.get("orm", self._detect_orm())

        dispatcher = ExpertDispatcher(self.project_dir)
        output_dir = self.project_dir / "output"
        output_dir.mkdir(exist_ok=True)
        saved = []
        step_errors: list[str] = []

        # CI/CD 配置
        try:
            cicd_output = dispatcher.dispatch_cicd(
                name=name,
                tech_stack=tech_stack,
                cicd_platform=cicd_platform,
            )
            cicd_path = output_dir / f"{name}-cicd.md"
            cicd_path.write_text(cicd_output.content, encoding="utf-8")
            saved.append(str(cicd_path))
            generated = cicd_output.metadata.get("generated_file_contents", {})
            saved.extend(self._write_generated_files(generated))
        except Exception as e:
            msg = f"CI/CD 配置生成失败: {e}"
            self.logger.error(msg)
            step_errors.append(msg)

        # 数据库迁移
        try:
            migration_output = dispatcher.dispatch_migration(
                name=name,
                tech_stack=tech_stack,
                orm=orm,
            )
            migration_path = output_dir / f"{name}-migration.md"
            migration_path.write_text(migration_output.content, encoding="utf-8")
            saved.append(str(migration_path))
            generated = migration_output.metadata.get("generated_file_contents", {})
            saved.extend(self._write_generated_files(generated))
        except Exception as e:
            msg = f"数据库迁移脚本生成失败: {e}"
            self.logger.error(msg)
            step_errors.append(msg)

        # 交付包
        try:
            packager = DeliveryPackager(
                project_dir=self.project_dir,
                name=name,
                version=__version__,
            )
            delivery_outputs = packager.package(cicd_platform=cicd_platform)
            saved.append(str(delivery_outputs["manifest_file"]))
            saved.append(str(delivery_outputs["report_file"]))
            saved.append(str(delivery_outputs["archive_file"]))
        except Exception as e:
            msg = f"交付包生成失败: {e}"
            self.logger.error(msg)
            step_errors.append(msg)

        if step_errors:
            raise PhaseExecutionError(
                phase=Phase.DEPLOYMENT.value.upper(),
                message="Deployment phase failed",
                details={"errors": step_errors},
            )

        return {
            "status": "deployment_ready",
            "cicd_platform": cicd_platform,
            "orm": orm,
            "files": saved,
        }

    # ==================== 联网检索 ====================

    async def _web_search(self, query: str, max_results: int = 5) -> list[dict]:
        """
        联网检索（Tavily > DDGS 文本搜索 > Instant Answer API > 离线模式）

        中国大陆环境：DuckDuckGo API 通常可访问；
        如需国内可访问的备选，可配置 TAVILY_API_KEY。
        """
        # 优先尝试 Tavily（如配置了 API Key）
        import os

        tavily_key = os.environ.get("TAVILY_API_KEY") or os.environ.get("SUPER_DEV_TAVILY_KEY")
        if tavily_key:
            try:
                return await self._search_tavily(query, tavily_key, max_results)
            except Exception as e:
                self.logger.debug(f"Tavily 检索失败，降级到 DuckDuckGo: {e}")

        # 降级到 DuckDuckGo（无需 API Key）
        try:
            return await self._search_duckduckgo(query, max_results)
        except Exception as e:
            self.logger.debug(f"DuckDuckGo 检索失败，进入完全离线模式: {e}")
            return []

    async def _search_duckduckgo(self, query: str, max_results: int = 5) -> list[dict]:
        """使用 DDGS 文本搜索 API 检索，回退到 Instant Answer API"""
        loop = asyncio.get_running_loop()

        # 优先使用 ddgs 库（真正的文本搜索，返回完整搜索结果）
        try:
            from ddgs import DDGS  # type: ignore[import-untyped]

            def _ddgs_search() -> list[dict]:
                ddgs = DDGS()
                entries = ddgs.text(query, max_results=max_results)
                items: list[dict] = []
                for entry in entries:
                    if not isinstance(entry, dict):
                        continue
                    items.append(
                        {
                            "title": str(entry.get("title", "")).strip(),
                            "snippet": str(entry.get("body", "")).strip()[:300],
                            "url": str(entry.get("href", "")).strip(),
                            "source": "DuckDuckGo",
                        }
                    )
                return items

            results = await loop.run_in_executor(None, _ddgs_search)
            if results:
                return results[:max_results]
        except ImportError:
            self.logger.debug("ddgs 库未安装，降级到 Instant Answer API")
        except Exception as e:
            self.logger.debug(f"DDGS 文本搜索失败，降级到 Instant Answer API: {e}")

        # 回退：DuckDuckGo Instant Answer API（百科摘要型，竞品搜索效果差）
        import urllib.parse

        encoded_query = urllib.parse.quote(query)
        url = f"https://api.duckduckgo.com/?q={encoded_query}&format=json&no_html=1&skip_disambig=1"

        response_text = await loop.run_in_executor(None, lambda: self._http_get(url, timeout=5))

        if not response_text:
            return []

        data = json.loads(response_text)
        results = []

        # 摘要结果
        if data.get("Abstract"):
            results.append(
                {
                    "title": data.get("Heading", query),
                    "snippet": data.get("Abstract", ""),
                    "url": data.get("AbstractURL", ""),
                    "source": "DuckDuckGo",
                }
            )

        # 相关主题
        for topic in data.get("RelatedTopics", [])[: max_results - 1]:
            if isinstance(topic, dict) and topic.get("Text"):
                results.append(
                    {
                        "title": topic.get("Text", "")[:80],
                        "snippet": topic.get("Text", ""),
                        "url": topic.get("FirstURL", ""),
                        "source": "DuckDuckGo",
                    }
                )

        return results[:max_results]

    async def _search_tavily(self, query: str, api_key: str, max_results: int = 5) -> list[dict]:
        """使用 Tavily API 进行深度检索"""
        import json as json_mod

        payload = json_mod.dumps(
            {
                "api_key": api_key,
                "query": query,
                "search_depth": "basic",
                "max_results": max_results,
            }
        ).encode("utf-8")

        loop = asyncio.get_running_loop()
        response_text = await loop.run_in_executor(
            None,
            lambda: self._http_post(
                "https://api.tavily.com/search",
                payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            ),
        )

        if not response_text:
            return []

        data = json.loads(response_text)
        results = []
        for item in data.get("results", [])[:max_results]:
            results.append(
                {
                    "title": item.get("title", ""),
                    "snippet": item.get("content", "")[:300],
                    "url": item.get("url", ""),
                    "source": "Tavily",
                }
            )
        return results

    def _http_get(self, url: str, timeout: int = 5) -> str | None:
        """同步 HTTP GET（在 executor 中执行）"""
        import urllib.request
        from urllib.parse import urlparse

        try:
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                return None
            req = urllib.request.Request(url, headers={"User-Agent": "super-dev/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
                raw: bytes = resp.read()
                decoded = raw.decode("utf-8", errors="ignore")
                return str(decoded)
        except Exception:
            return None

    def _http_post(self, url: str, data: bytes, headers: dict, timeout: int = 10) -> str | None:
        """同步 HTTP POST（在 executor 中执行）"""
        import urllib.request
        from urllib.parse import urlparse

        try:
            parsed = urlparse(url)
            if parsed.scheme not in {"http", "https"}:
                return None
            req = urllib.request.Request(url, data=data, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # nosec B310
                raw: bytes = resp.read()
                decoded = raw.decode("utf-8", errors="ignore")
                return str(decoded)
        except Exception:
            return None

    def _detect_orm(self) -> str:
        """自动检测项目使用的 ORM"""
        if (self.project_dir / "prisma").exists():
            return "prisma"
        if (self.project_dir / "requirements.txt").exists():
            req = (self.project_dir / "requirements.txt").read_text(
                encoding="utf-8", errors="ignore"
            )
            if "sqlalchemy" in req.lower():
                return "sqlalchemy"
            if "django" in req.lower():
                return "django"
        if (self.project_dir / "package.json").exists():
            pkg = (self.project_dir / "package.json").read_text(encoding="utf-8", errors="ignore")
            if "typeorm" in pkg.lower():
                return "typeorm"
            if "sequelize" in pkg.lower():
                return "sequelize"
            if "mongoose" in pkg.lower():
                return "mongoose"
        return "prisma"

    # ==================== 打印方法 ====================

    def _print_workflow_start(self, phases: list[Phase]) -> None:
        if self.console:
            try:
                from ..branding import banner, progress_bar

                self.console.print(banner(f"项目: {self.config_manager.config.name}"))
                self.console.print(progress_bar(0, len(phases), "准备中..."))
            except ImportError:
                self.console.print(
                    Panel.fit(
                        f"[bold cyan]Super Dev 工作流引擎[/bold cyan]\n\n"
                        f"项目: {self.config_manager.config.name}\n"
                        f"阶段: {len(phases)} 个",
                        title="启动",
                    )
                )

    def _print_phase_start(self, phase: Phase, phase_index: int, total: int) -> None:
        """输出阶段开始标识。"""
        if self.console:
            try:
                from ..branding import phase_start, progress_bar

                self.console.print(phase_start(phase.value))
                self.console.print(progress_bar(phase_index, total, phase.value))
            except ImportError:
                self.console.print(f"\n[cyan]▶[/cyan] {phase.value}: 开始执行...")

    def _print_phase_complete(self, phase: Phase, result: PhaseResult) -> None:
        if self.console:
            try:
                from ..branding import phase_complete

                self.console.print(
                    phase_complete(phase.value, result.success, result.quality_score)
                )
            except ImportError:
                self.console.print(
                    f"[green]✓[/green] {phase.value}: "
                    f"完成 ({result.duration:.1f}s, 质量分: {result.quality_score:.0f})"
                )

    def _print_phase_failed(self, phase: Phase, result: PhaseResult) -> None:
        if self.console:
            self.console.print(f"[red]✗[/red] {phase.value}: " f"失败 ({', '.join(result.errors)})")

    def _print_quality_gate_failed(self, phase: Phase, result: PhaseResult) -> None:
        if self.console:
            gate = self.config_manager.config.quality_gate
            self.console.print(
                f"[yellow]⚠[/yellow] {phase.value}: "
                f"质量分 ({result.quality_score:.0f}) 低于门禁 ({gate})"
            )

    def _print_workflow_complete(self, results: dict[Phase, PhaseResult]) -> None:
        if self.console:
            table = Table(title="工作流执行结果")
            table.add_column("阶段", style="cyan")
            table.add_column("状态", style="green")
            table.add_column("耗时", style="yellow")
            table.add_column("质量分", style="magenta")

            total_duration = 0.0
            success_count = 0

            for phase, result in results.items():
                status = "[green]成功[/green]" if result.success else "[red]失败[/red]"
                duration = f"{result.duration:.1f}s"
                quality = f"{result.quality_score:.0f}"

                table.add_row(phase.value, status, duration, quality)
                total_duration += result.duration
                if result.success:
                    success_count += 1

            self.console.print(table)
            self.console.print(
                f"\n总计: {success_count}/{len(results)} 成功, " f"总耗时: {total_duration:.1f}s"
            )

    def _save_report(self, results: dict[Phase, PhaseResult]) -> None:
        output_dir = self.project_dir / self.config_manager.config.output_dir
        output_dir.mkdir(exist_ok=True)

        report_path = (
            output_dir / f"workflow_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        report_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "project": self.config_manager.config.name,
            "results": {
                phase.value: {
                    "success": result.success,
                    "duration": result.duration,
                    "quality_score": result.quality_score,
                    "errors": result.errors,
                }
                for phase, result in results.items()
            },
        }

        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

    def _seed_context_user_input(self, context: WorkflowContext) -> None:
        cfg = self.config_manager.config
        defaults = {
            "name": cfg.name or self.project_dir.name,
            "description": cfg.description,
            "platform": cfg.platform,
            "frontend": cfg.frontend,
            "backend": cfg.backend,
            "domain": cfg.domain,
            "cicd": "github",
            "offline": False,
        }
        for key, value in defaults.items():
            context.user_input.setdefault(key, value)

    def _write_generated_files(self, generated_files: Any) -> list[str]:
        if not isinstance(generated_files, dict):
            return []
        saved_paths: list[str] = []
        for relative_path, content in generated_files.items():
            if not isinstance(relative_path, str) or not isinstance(content, str):
                continue
            full_path = self.project_dir / relative_path
            full_path.parent.mkdir(parents=True, exist_ok=True)
            full_path.write_text(content, encoding="utf-8")
            saved_paths.append(str(full_path))
        return saved_paths
