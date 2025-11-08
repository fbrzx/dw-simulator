"""
High-level orchestration service for experiment lifecycle operations.

This module bridges schema validation with the persistence layer so the CLI
and future API surfaces can reuse consistent business logic.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import shutil
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Sequence, Set

from pydantic import ValidationError

from .config import get_data_root
from .persistence import (
    DataLoadError,
    ExperimentAlreadyExistsError,
    ExperimentMaterializationError,
    ExperimentMetadata,
    ExperimentNotFoundError,
    ExperimentPersistence,
    GenerationAlreadyRunningError,
    GenerationRunMetadata,
    GenerationRunNotFoundError,
    GenerationStatus,
    QueryExecutionError,
    QueryResult,
)
from .schema import ExperimentSchema, parse_experiment_schema, validate_experiment_payload
from .generator import (
    ExperimentGenerator,
    GenerationRequest,
    GenerationResult,
    GenerationError,
)
from .sql_importer import import_sql, SqlImportOptions, SqlImportError, DEFAULT_TARGET_ROWS, SUPPORTED_DIALECTS

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExperimentCreateResult:
    """Return object describing creation attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    metadata: ExperimentMetadata | None = None
    warnings: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class ExperimentDeleteResult:
    """Outcome for experiment deletion attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    deleted_tables: int = 0


@dataclass(frozen=True)
class ExperimentResetResult:
    """Outcome for experiment reset attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    reset_tables: int = 0


@dataclass(frozen=True)
class ExperimentLoadResult:
    """Outcome for experiment data loading attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    loaded_tables: int = 0
    row_counts: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryExecutionResult:
    """Outcome for query execution attempts."""

    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    result: QueryResult | None = None


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
        target_warehouse: str | None = None,
    ) -> ExperimentCreateResult:
        """Import SQL DDL and create an experiment."""

        try:
            schema = import_sql(
                sql,
                SqlImportOptions(
                    experiment_name=name,
                    dialect=dialect,
                    default_target_rows=target_rows or DEFAULT_TARGET_ROWS,
                    target_warehouse=target_warehouse,
                ),
            )
        except SqlImportError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])

        # Extract warnings from schema
        warnings = self._extract_warnings_from_schema(schema)

        try:
            metadata = self.persistence.create_experiment(schema)
            return ExperimentCreateResult(success=True, metadata=metadata, warnings=warnings)
        except ExperimentAlreadyExistsError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentCreateResult(success=False, errors=[str(exc)])

    def list_experiments(self) -> list[ExperimentMetadata]:
        """Return all experiment metadata entries."""

        return self.persistence.list_experiments()

    def delete_experiment(self, name: str) -> ExperimentDeleteResult:
        """Delete metadata + physical tables for the specified experiment."""

        data_root = get_data_root()
        runs = self.persistence.list_generation_runs(name)
        artifact_paths = self._collect_generated_artifact_paths(name, runs, data_root)

        try:
            deleted_tables = self.persistence.delete_experiment(name)
        except ExperimentNotFoundError as exc:
            return ExperimentDeleteResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentDeleteResult(success=False, errors=[str(exc)])

        self._delete_generated_artifacts(artifact_paths, data_root)
        return ExperimentDeleteResult(success=True, deleted_tables=deleted_tables)

    def reset_experiment(self, name: str) -> ExperimentResetResult:
        """
        Reset an experiment by truncating all tables without deleting the schema.
        Guards against resetting while generation is active.
        """
        try:
            reset_tables = self.persistence.reset_experiment(name)
        except ExperimentNotFoundError as exc:
            return ExperimentResetResult(success=False, errors=[str(exc)])
        except GenerationAlreadyRunningError as exc:
            return ExperimentResetResult(success=False, errors=[str(exc)])
        except ExperimentMaterializationError as exc:
            return ExperimentResetResult(success=False, errors=[str(exc)])

        return ExperimentResetResult(success=True, reset_tables=reset_tables)

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

        # Resolve the output directory so both the generator and persistence
        # record the same location.
        data_root = get_data_root()
        if output_dir is not None:
            effective_output_dir = Path(output_dir)
        else:
            timestamp = int(time.time())
            effective_output_dir = data_root / "generated" / experiment_name / str(timestamp)

        # Start generation run with concurrent job guard
        try:
            run_id = self.persistence.start_generation_run(
                experiment_name=experiment_name,
                output_path=str(effective_output_dir),
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
                    output_root=effective_output_dir,
                    row_overrides={k: int(v) for k, v in (rows or {}).items()},
                    seed=seed,
                )
            )

            # Build row counts JSON
            row_counts_dict = {
                table_result.table_name: table_result.row_count
                for table_result in summary.tables
            }
            # Mark run as generated (before loading). Store generated counts.
            self.persistence.complete_generation_run(
                run_id,
                json.dumps({"generated": row_counts_dict}),
            )

            # Attempt to load Parquet files into the warehouse tables.
            try:
                loaded_row_counts = self.persistence.load_generation_run(run_id)
            except (ExperimentNotFoundError, DataLoadError) as load_exc:
                error_message = f"Data load failed for experiment '{experiment_name}': {load_exc}"
                try:
                    self.persistence.fail_generation_run(run_id, error_message)
                except Exception:
                    pass

                run_metadata = self.persistence.get_generation_run(run_id)
                return ExperimentGenerateResult(
                    success=False,
                    errors=[error_message],
                    summary=summary,
                    run_metadata=run_metadata,
                    run_id=run_id,
                )

            # Persist combined generated + loaded row counts for observability.
            combined_counts = {
                "generated": row_counts_dict,
                "loaded": loaded_row_counts,
            }
            self.persistence.complete_generation_run(run_id, json.dumps(combined_counts))

            # Fetch the completed run metadata (now including load info)
            run_metadata = self.persistence.get_generation_run(run_id)

            return ExperimentGenerateResult(
                success=True,
                summary=summary,
                run_metadata=run_metadata,
                loaded_row_counts=loaded_row_counts,
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

    def load_experiment_data(
        self,
        experiment_name: str,
        run_id: int | None = None,
    ) -> ExperimentLoadResult:
        """
        Load Parquet files from a generation run into warehouse tables.

        If no run_id is provided, loads the most recent completed generation run.

        Args:
            experiment_name: Name of the experiment to load
            run_id: Specific generation run ID to load (optional)

        Returns:
            ExperimentLoadResult with success status, errors, and row counts
        """
        # Verify experiment exists
        metadata = self.persistence.get_experiment_metadata(experiment_name)
        if metadata is None:
            return ExperimentLoadResult(
                success=False,
                errors=[f"Experiment '{experiment_name}' does not exist."]
            )

        # If no run_id provided, find the most recent completed run
        if run_id is None:
            runs = self.persistence.list_generation_runs(experiment_name)
            completed_runs = [
                run for run in runs
                if run.status == GenerationStatus.COMPLETED
            ]

            if not completed_runs:
                return ExperimentLoadResult(
                    success=False,
                    errors=[f"No completed generation runs found for experiment '{experiment_name}'."]
                )

            # list_generation_runs returns runs in reverse chronological order (most recent first)
            run_id = completed_runs[0].id

        # Attempt to load the specified run
        try:
            row_counts = self.persistence.load_generation_run(run_id)
            return ExperimentLoadResult(
                success=True,
                loaded_tables=len(row_counts),
                row_counts=row_counts,
            )
        except GenerationRunNotFoundError as exc:
            return ExperimentLoadResult(
                success=False,
                errors=[str(exc)]
            )
        except DataLoadError as exc:
            return ExperimentLoadResult(
                success=False,
                errors=[str(exc)]
            )
        except ExperimentNotFoundError as exc:
            return ExperimentLoadResult(
                success=False,
                errors=[str(exc)]
            )
        except Exception as exc:
            return ExperimentLoadResult(
                success=False,
                errors=[f"Unexpected error during data loading: {type(exc).__name__}: {exc}"]
            )

    def list_generation_runs(self, experiment_name: str) -> list[GenerationRunMetadata]:
        """List all generation runs for an experiment."""
        return self.persistence.list_generation_runs(experiment_name)

    def get_generation_run(self, run_id: int) -> GenerationRunMetadata | None:
        """Get metadata for a specific generation run."""
        return self.persistence.get_generation_run(run_id)

    def execute_query(self, sql: str, experiment_name: str | None = None) -> QueryExecutionResult:
        """
        Execute a SQL query and return the results.

        If experiment_name is provided, creates temporary views for the experiment's tables,
        allowing queries using simple table names (e.g., "customers") instead of prefixed names
        (e.g., "experiment__customers").

        Handles errors and provides user-friendly feedback.
        """
        try:
            result = self.persistence.execute_query(sql, experiment_name=experiment_name)
            return QueryExecutionResult(success=True, result=result)
        except QueryExecutionError as exc:
            return QueryExecutionResult(success=False, errors=[str(exc)])
        except Exception as exc:
            return QueryExecutionResult(
                success=False,
                errors=[f"Unexpected error during query execution: {exc}"]
            )

    @staticmethod
    def export_query_results_to_csv(result: QueryResult) -> str:
        """
        Export query results to CSV format.
        Returns a CSV string that can be written to a file or returned via API.
        """
        output = io.StringIO()
        writer = csv.writer(output)

        # Write header
        writer.writerow(result.columns)

        # Write data rows
        for row in result.rows:
            writer.writerow(row)

        return output.getvalue()

    @staticmethod
    def save_query_to_file(sql: str, file_path: Path) -> None:
        """
        Save SQL query text to a .sql file.
        """
        file_path.write_text(sql)

    # Internal helpers -------------------------------------------------

    def _extract_warnings_from_schema(self, schema: ExperimentSchema) -> list[str]:
        """Extract all warnings from all tables in a schema."""
        warnings = []
        for table in schema.tables:
            if table.warnings:
                warnings.extend(table.warnings)
        return warnings

    def get_experiment_warnings(self, experiment_name: str) -> list[str]:
        """Get warnings for an existing experiment by loading its schema."""
        metadata = self.persistence.get_experiment_metadata(experiment_name)
        if metadata is None:
            return []

        try:
            schema = ExperimentSchema.model_validate_json(metadata.schema_json)
            return self._extract_warnings_from_schema(schema)
        except (ValidationError, ValueError):
            return []

    @staticmethod
    def summarize_distribution_configs(schema: ExperimentSchema) -> list[dict[str, Any]]:
        """Return distribution summaries for columns configured with statistical distributions."""

        summaries: list[dict[str, Any]] = []
        for table in schema.tables:
            for column in table.columns:
                if column.distribution is None:
                    continue

                summaries.append(
                    {
                        "table": table.name,
                        "column": column.name,
                        "type": column.distribution.type,
                        "parameters": {
                            key: column.distribution.parameters[key]
                            for key in sorted(column.distribution.parameters)
                        },
                    }
                )

        return summaries

    def _collect_generated_artifact_paths(
        self,
        experiment_name: str,
        runs: Sequence[GenerationRunMetadata],
        data_root: Path,
    ) -> Set[Path]:
        """Collect filesystem paths that should be pruned when deleting an experiment."""

        paths: Set[Path] = set()

        default_path = data_root / "generated" / experiment_name
        paths.add(default_path)

        resolved_root = data_root.resolve()

        for run in runs:
            if not run.output_path:
                continue

            candidate = Path(run.output_path).expanduser()
            try:
                resolved_candidate = candidate.resolve()
            except FileNotFoundError:
                resolved_candidate = candidate

            if self._is_within_data_root(resolved_candidate, resolved_root):
                paths.add(resolved_candidate)

        return paths

    def _delete_generated_artifacts(self, paths: Set[Path], data_root: Path) -> None:
        """Best-effort deletion of generated Parquet folders."""

        resolved_root = data_root.resolve()
        for path in paths:
            try:
                resolved_path = path.resolve()
            except FileNotFoundError:
                resolved_path = path

            if not self._is_within_data_root(resolved_path, resolved_root):
                logger.warning("Skipping deletion outside data root: %s", resolved_path)
                continue

            if not resolved_path.exists():
                continue

            try:
                shutil.rmtree(resolved_path)
            except FileNotFoundError:
                continue
            except Exception as exc:  # pragma: no cover - safety belt
                logger.warning("Failed to delete generated data at %s: %s", resolved_path, exc)

    @staticmethod
    def _is_within_data_root(path: Path, data_root: Path) -> bool:
        """Return True when the target resides inside the configured data root."""

        try:
            path.relative_to(data_root)
            return True
        except ValueError:
            return False


@dataclass(frozen=True)
class ExperimentGenerateResult:
    success: bool
    errors: Sequence[str] = field(default_factory=tuple)
    summary: GenerationResult | None = None
    run_metadata: GenerationRunMetadata | None = None
    run_id: int | None = None
    loaded_row_counts: Mapping[str, int] | None = None


__all__ = [
    "ExperimentService",
    "ExperimentCreateResult",
    "ExperimentDeleteResult",
    "ExperimentResetResult",
    "ExperimentLoadResult",
    "ExperimentGenerateResult",
    "QueryExecutionResult",
    "SUPPORTED_DIALECTS",
]
