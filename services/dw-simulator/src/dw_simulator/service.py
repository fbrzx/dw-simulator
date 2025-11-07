"""
High-level orchestration service for experiment lifecycle operations.

This module bridges schema validation with the persistence layer so the CLI
and future API surfaces can reuse consistent business logic.
"""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence

from pydantic import ValidationError

from .persistence import (
    ExperimentAlreadyExistsError,
    ExperimentMaterializationError,
    ExperimentMetadata,
    ExperimentNotFoundError,
    ExperimentPersistence,
    GenerationAlreadyRunningError,
    GenerationRunMetadata,
)
from .schema import ExperimentSchema, parse_experiment_schema, validate_experiment_payload
from .generator import (
    ExperimentGenerator,
    GenerationRequest,
    GenerationResult,
    GenerationError,
)
from .sql_importer import import_sql, SqlImportOptions, SqlImportError, DEFAULT_TARGET_ROWS, SUPPORTED_DIALECTS


@dataclass(frozen=True)
class ExperimentCreateResult:
    """Return object describing creation attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    metadata: ExperimentMetadata | None = None


@dataclass(frozen=True)
class ExperimentDeleteResult:
    """Outcome for experiment deletion attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    deleted_tables: int = 0


class ExperimentService:
    """Coordinates schema validation with persistence."""

    def __init__(
        self,
        persistence: ExperimentPersistence | None = None,
        generator: ExperimentGenerator | None = None,
    ) -> None:
        self.persistence = persistence or ExperimentPersistence()
        self.generator = generator or ExperimentGenerator()

    def create_experiment_from_payload(
        self, payload: Mapping[str, Any] | str
    ) -> ExperimentCreateResult:
        """
        Validate incoming payloads and persist experiments if possible.
        """

        validation = validate_experiment_payload(payload)
        if not validation.is_valid:
            return ExperimentCreateResult(success=False, errors=validation.errors)

        try:
            schema = parse_experiment_schema(payload)
            metadata = self.persistence.create_experiment(schema)
            return ExperimentCreateResult(success=True, metadata=metadata)
        except ExperimentAlreadyExistsError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])
        except (ValidationError, ValueError, TypeError) as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])

    def create_experiment_from_file(self, path: Path) -> ExperimentCreateResult:
        """Load a JSON schema file and proxy to the payload creation flow."""

        try:
            content = path.read_text()
        except OSError as exc:
            return ExperimentCreateResult(success=False, errors=[f"Failed to read schema file: {exc}"])

        try:
            payload = json.loads(content)
        except json.JSONDecodeError as exc:
            return ExperimentCreateResult(success=False, errors=[f"Invalid JSON: {exc}"])

        return self.create_experiment_from_payload(payload)

    def create_experiment_from_sql(
        self,
        name: str,
        sql: str,
        dialect: str = "ansi",
        target_rows: int | None = None,
    ) -> ExperimentCreateResult:
        """Import SQL DDL and create an experiment."""

        try:
            schema = import_sql(
                sql,
                SqlImportOptions(
                    experiment_name=name,
                    dialect=dialect,
                    default_target_rows=target_rows or DEFAULT_TARGET_ROWS,
                ),
            )
        except SqlImportError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])

        try:
            metadata = self.persistence.create_experiment(schema)
            return ExperimentCreateResult(success=True, metadata=metadata)
        except ExperimentAlreadyExistsError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])

    def list_experiments(self) -> list[ExperimentMetadata]:
        """Return all experiment metadata entries."""

        return self.persistence.list_experiments()

    def delete_experiment(self, name: str) -> ExperimentDeleteResult:
        """Delete metadata + physical tables for the specified experiment."""

        try:
            deleted_tables = self.persistence.delete_experiment(name)
            return ExperimentDeleteResult(success=True, deleted_tables=deleted_tables)
        except ExperimentNotFoundError as exc:
            return ExperimentDeleteResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentDeleteResult(success=False, errors=[str(exc)])

    def generate_data(
        self,
        experiment_name: str,
        rows: Mapping[str, int] | None = None,
        seed: int | None = None,
        output_dir: Path | None = None,
    ) -> "ExperimentGenerateResult":
        """
        Generate synthetic data for an experiment with run tracking and concurrent guards.
        """
        metadata = self.persistence.get_experiment_metadata(experiment_name)
        if metadata is None:
            return ExperimentGenerateResult(
                success=False,
                errors=[f"Experiment '{experiment_name}' does not exist."]
            )

        try:
            schema = ExperimentSchema.model_validate_json(metadata.schema_json)
        except ValidationError as exc:
            return ExperimentGenerateResult(
                success=False,
                errors=[f"Invalid schema stored for '{experiment_name}': {exc}"]
            )

        # Start generation run with concurrent job guard
        try:
            run_id = self.persistence.start_generation_run(
                experiment_name=experiment_name,
                output_path=str(output_dir) if output_dir else None,
                seed=seed,
            )
        except GenerationAlreadyRunningError as exc:
            return ExperimentGenerateResult(
                success=False,
                errors=[str(exc)]
            )
        except (ExperimentNotFoundError, ExperimentMaterializationError) as exc:
            return ExperimentGenerateResult(
                success=False,
                errors=[f"Failed to start generation run: {exc}"]
            )

        # Execute generation with error tracking
        try:
            summary = self.generator.generate(
                GenerationRequest(
                    schema=schema,
                    output_root=output_dir,
                    row_overrides={k: int(v) for k, v in (rows or {}).items()},
                    seed=seed,
                )
            )

            # Build row counts JSON
            row_counts_dict = {
                table_result.table_name: table_result.row_count
                for table_result in summary.tables
            }
            row_counts_json = json.dumps(row_counts_dict)

            # Mark run as completed
            self.persistence.complete_generation_run(run_id, row_counts_json)

            # Fetch the completed run metadata
            run_metadata = self.persistence.get_generation_run(run_id)

            return ExperimentGenerateResult(
                success=True,
                summary=summary,
                run_metadata=run_metadata,
            )

        except (GenerationError, ValueError, TypeError) as exc:
            # Richer error reporting: capture full traceback
            error_message = f"{type(exc).__name__}: {exc}\n\nTraceback:\n{traceback.format_exc()}"
            try:
                self.persistence.fail_generation_run(run_id, error_message)
            except Exception as persist_exc:
                # If we can't persist the failure, include it in the error
                error_message += f"\n\nAdditionally, failed to persist error: {persist_exc}"

            return ExperimentGenerateResult(
                success=False,
                errors=[error_message],
                run_id=run_id,
            )
        except Exception as exc:
            # Catch-all for unexpected errors
            error_message = f"Unexpected error during generation: {type(exc).__name__}: {exc}\n\nTraceback:\n{traceback.format_exc()}"
            try:
                self.persistence.fail_generation_run(run_id, error_message)
            except Exception:
                pass  # Best effort

            return ExperimentGenerateResult(
                success=False,
                errors=[error_message],
                run_id=run_id,
            )

    def list_generation_runs(self, experiment_name: str) -> list[GenerationRunMetadata]:
        """List all generation runs for an experiment."""
        return self.persistence.list_generation_runs(experiment_name)

    def get_generation_run(self, run_id: int) -> GenerationRunMetadata | None:
        """Get metadata for a specific generation run."""
        return self.persistence.get_generation_run(run_id)


@dataclass(frozen=True)
class ExperimentGenerateResult:
    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    summary: GenerationResult | None = None
    run_metadata: GenerationRunMetadata | None = None
    run_id: int | None = None


__all__ = [
    "ExperimentService",
    "ExperimentCreateResult",
    "ExperimentDeleteResult",
    "ExperimentGenerateResult",
    "SUPPORTED_DIALECTS",
]
