"""FastAPI application exposing experiment lifecycle endpoints."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel, Field, ValidationError

from . import __version__
from .service import (
    ExperimentCreateResult,
    ExperimentDeleteResult,
    ExperimentResetResult,
    ExperimentGenerateResult,
    ExperimentLoadResult,
    ExperimentService,
    QueryExecutionResult,
    SUPPORTED_DIALECTS,
)
from .schema import ExperimentSchema


ALLOWED_ORIGINS = [
    "http://localhost:4173",
    "http://127.0.0.1:4173",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8000",
]


class GeneratePayload(BaseModel):
    rows: dict[str, int] | None = Field(default=None, description="Optional row overrides per table.")
    seed: int | None = Field(default=None, description="Optional RNG seed.")
    output_dir: str | None = Field(default=None, description="Optional output directory for Parquet files.")


class LoadPayload(BaseModel):
    run_id: int | None = Field(default=None, description="Specific generation run ID to load (defaults to most recent).")


class SqlImportPayload(BaseModel):
    name: str = Field(..., description="Experiment name to persist.")
    sql: str = Field(..., description="SQL text containing CREATE TABLE statements.")
    dialect: str = Field("redshift", description=f"SQL dialect ({', '.join(SUPPORTED_DIALECTS)}).")
    target_rows: int | None = Field(None, ge=1, description="Default target row count per table.")
    target_warehouse: str | None = Field(None, description="Target warehouse type (sqlite/redshift/snowflake). Defaults to system default.")


class QueryExecutePayload(BaseModel):
    sql: str = Field(..., description="SQL query to execute.")
    format: str = Field("json", description="Result format: 'json' or 'csv'.")
    experiment_name: str | None = Field(None, description="Optional experiment name to create temporary views for simplified querying.")


def create_app(service: ExperimentService | None = None) -> FastAPI:
    experiment_service = service or ExperimentService()
    app = FastAPI(title="DW Simulator API", version=__version__)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.state.experiment_service = experiment_service

    def _service() -> ExperimentService:
        return app.state.experiment_service

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/experiments")
    def list_experiments() -> dict[str, Any]:
        experiments = _service().list_experiments()
        summaries = []
        for experiment in experiments:
            table_count = _service().persistence.get_table_count(experiment.name)
            warnings = _service().get_experiment_warnings(experiment.name)
            distributions: list[dict[str, Any]] = []
            try:
                schema = ExperimentSchema.model_validate_json(experiment.schema_json)
                distributions = ExperimentService.summarize_distribution_configs(schema)
            except (ValidationError, ValueError):
                distributions = []
            # Include schema for UI to show table details
            summaries.append(
                {
                    "name": experiment.name,
                    "description": experiment.description,
                    "created_at": experiment.created_at.isoformat(),
                    "table_count": table_count,
                    "schema": experiment.schema_json,
                    "warnings": warnings,
                    "warehouse_type": experiment.warehouse_type,
                    "distributions": distributions,
                }
            )
        return {"experiments": summaries}

    @app.post("/api/experiments", status_code=status.HTTP_201_CREATED)
    def create_experiment(schema: dict[str, Any]) -> dict[str, Any]:
        result = _service().create_experiment_from_payload(schema)
        if not result.success or not result.metadata:
            raise HTTPException(
                status_code=_http_status_for_errors(result),
                detail=result.errors,
            )
        return {
            "name": result.metadata.name,
            "description": result.metadata.description,
            "created_at": result.metadata.created_at.isoformat(),
            "warehouse_type": result.metadata.warehouse_type,
        }

    @app.delete("/api/experiments/{name}")
    def delete_experiment(name: str) -> dict[str, Any]:
        result = _service().delete_experiment(name)
        if not result.success:
            raise HTTPException(
                status_code=_http_status_for_errors(result),
                detail=result.errors,
            )
        return {"name": name, "dropped_tables": result.deleted_tables}

    @app.post("/api/experiments/{name}/reset")
    def reset_experiment(name: str) -> dict[str, Any]:
        """Reset an experiment by truncating all tables without deleting the schema."""
        result = _service().reset_experiment(name)
        if not result.success:
            raise HTTPException(
                status_code=_http_status_for_errors(result),
                detail=result.errors,
            )
        return {"name": name, "reset_tables": result.reset_tables}

    @app.post("/api/experiments/{name}/generate", status_code=status.HTTP_202_ACCEPTED)
    def generate_experiment(name: str, payload: GeneratePayload) -> dict[str, Any]:
        result = _service().generate_data(
            experiment_name=name,
            rows=payload.rows,
            seed=payload.seed,
            output_dir=Path(payload.output_dir) if payload.output_dir else None,
        )
        if not result.success or not result.summary:
            raise HTTPException(
                status_code=_http_status_for_errors(result),
                detail=result.errors,
            )
        return {
            "experiment": result.summary.experiment_name,
            "output_dir": str(result.summary.output_dir),
            "tables": [
                {"name": table.table_name, "row_count": table.row_count, "files": [str(f) for f in table.files]}
                for table in result.summary.tables
            ],
        }

    @app.post("/api/experiments/{name}/load")
    def load_experiment(name: str, payload: LoadPayload) -> dict[str, Any]:
        """Load Parquet files from a generation run into warehouse tables."""
        result = _service().load_experiment_data(
            experiment_name=name,
            run_id=payload.run_id,
        )
        if not result.success:
            raise HTTPException(
                status_code=_http_status_for_load_errors(result),
                detail=result.errors,
            )
        return {
            "experiment": name,
            "loaded_tables": result.loaded_tables,
            "row_counts": result.row_counts,
        }

    @app.get("/api/experiments/{name}/runs")
    def list_generation_runs(name: str) -> dict[str, Any]:
        """List all generation runs for an experiment, most recent first."""
        runs = _service().persistence.list_generation_runs(name)
        return {
            "runs": [
                {
                    "id": run.id,
                    "experiment_name": run.experiment_name,
                    "status": run.status.value,
                    "started_at": run.started_at.isoformat(),
                    "completed_at": run.completed_at.isoformat() if run.completed_at else None,
                    "row_counts": run.row_counts,
                    "output_path": run.output_path,
                    "error_message": run.error_message,
                    "seed": run.seed,
                }
                for run in runs
            ]
        }

    @app.get("/api/experiments/{name}/runs/{run_id}")
    def get_generation_run(name: str, run_id: int) -> dict[str, Any]:
        """Get details of a specific generation run."""
        run = _service().persistence.get_generation_run(run_id)
        if not run or run.experiment_name != name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=[f"Generation run {run_id} not found for experiment {name}"],
            )
        return {
            "id": run.id,
            "experiment_name": run.experiment_name,
            "status": run.status.value,
            "started_at": run.started_at.isoformat(),
            "completed_at": run.completed_at.isoformat() if run.completed_at else None,
            "row_counts": run.row_counts,
            "output_path": run.output_path,
            "error_message": run.error_message,
            "seed": run.seed,
        }

    @app.get("/api/experiments/{name}/lineage")
    def get_lineage(name: str) -> dict[str, Any]:
        """Get lineage graph data for an experiment (table relationships, FK dependencies)."""
        from .persistence import ExperimentNotFoundError
        try:
            graph = _service().persistence.build_lineage_graph(name)
            return {
                "experiment_name": graph.experiment_name,
                "graph": graph.to_dict(),
            }
        except ExperimentNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=[str(exc)],
            )

    @app.get("/api/experiments/{name}/lineage/export")
    def export_lineage(name: str) -> Response:
        """Export lineage graph as GraphViz DOT file."""
        from .lineage import export_lineage_dot
        from .persistence import ExperimentNotFoundError
        try:
            graph = _service().persistence.build_lineage_graph(name)
            dot_content = export_lineage_dot(graph, name)
            return Response(
                content=dot_content,
                media_type="text/vnd.graphviz",
                headers={
                    "Content-Disposition": f'attachment; filename="{name}_lineage.dot"'
                },
            )
        except ExperimentNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=[str(exc)],
            )

    @app.post("/api/experiments/import-sql", status_code=status.HTTP_201_CREATED)
    def import_sql_endpoint(payload: SqlImportPayload) -> dict[str, Any]:
        result = _service().create_experiment_from_sql(
            name=payload.name,
            sql=payload.sql,
            dialect=payload.dialect,
            target_rows=payload.target_rows,
            target_warehouse=payload.target_warehouse,
        )
        if not result.success or not result.metadata:
            raise HTTPException(
                status_code=_http_status_for_errors(result),
                detail=result.errors,
            )
        return {
            "name": result.metadata.name,
            "created_at": result.metadata.created_at.isoformat(),
            "dialect": payload.dialect,
            "warnings": list(result.warnings),
            "warehouse_type": result.metadata.warehouse_type,
        }

    @app.post("/api/query/execute", response_model=None)
    def execute_query(payload: QueryExecutePayload) -> dict[str, Any] | Response:
        """
        Execute a SQL query and return results in JSON or CSV format.

        If experiment_name is provided, creates temporary views for simplified querying,
        allowing table references without the experiment__ prefix.
        """
        result = _service().execute_query(payload.sql, experiment_name=payload.experiment_name)

        if not result.success or not result.result:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=result.errors,
            )

        # Return CSV format if requested
        if payload.format.lower() == "csv":
            csv_content = _service().export_query_results_to_csv(result.result)
            return Response(
                content=csv_content,
                media_type="text/csv",
                headers={"Content-Disposition": "attachment; filename=query_results.csv"}
            )

        # Default: Return JSON format
        return {
            "columns": result.result.columns,
            "rows": [list(row) for row in result.result.rows],
            "row_count": result.result.row_count,
        }

    return app


def _http_status_for_errors(
    result: ExperimentCreateResult | ExperimentDeleteResult | ExperimentResetResult | ExperimentGenerateResult,
) -> int:
    """Translate domain errors into HTTP statuses."""

    messages = " ".join(result.errors).lower()
    if "does not exist" in messages:
        return status.HTTP_404_NOT_FOUND
    if "already exists" in messages:
        return status.HTTP_409_CONFLICT
    if "already running" in messages or "generation is running" in messages:
        return status.HTTP_409_CONFLICT
    return status.HTTP_400_BAD_REQUEST


def _http_status_for_load_errors(result: ExperimentLoadResult) -> int:
    """Translate load errors into HTTP statuses."""

    messages = " ".join(result.errors).lower()
    if "does not exist" in messages:
        return status.HTTP_404_NOT_FOUND
    if "no completed generation runs" in messages:
        return status.HTTP_409_CONFLICT
    return status.HTTP_500_INTERNAL_SERVER_ERROR


app = create_app()
