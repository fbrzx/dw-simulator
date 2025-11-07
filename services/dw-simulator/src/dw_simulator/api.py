"""FastAPI application exposing experiment lifecycle endpoints."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from . import __version__
from .service import (
    ExperimentCreateResult,
    ExperimentDeleteResult,
    ExperimentGenerateResult,
    ExperimentService,
    SUPPORTED_DIALECTS,
)


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


class SqlImportPayload(BaseModel):
    name: str = Field(..., description="Experiment name to persist.")
    sql: str = Field(..., description="SQL text containing CREATE TABLE statements.")
    dialect: str = Field("redshift", description=f"SQL dialect ({', '.join(SUPPORTED_DIALECTS)}).")
    target_rows: int | None = Field(None, ge=1, description="Default target row count per table.")


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
        from .schema import ExperimentSchema
        from pydantic import ValidationError

        experiments = _service().list_experiments()
        summaries = []
        for experiment in experiments:
            table_names = _service().persistence.list_tables(experiment.name)

            # Extract warnings from schema
            warnings = []
            try:
                schema = ExperimentSchema.model_validate_json(experiment.schema_json)
                for table in schema.tables:
                    warnings.extend(table.warnings)
            except (ValidationError, ValueError):
                # If schema parsing fails, just skip warnings
                pass

            # Include schema for UI to show table details
            summaries.append(
                {
                    "name": experiment.name,
                    "description": experiment.description,
                    "created_at": experiment.created_at.isoformat(),
                    "table_count": len(table_names),
                    "schema": experiment.schema_json,
                    "warnings": warnings,
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

    @app.post("/api/experiments/import-sql", status_code=status.HTTP_201_CREATED)
    def import_sql_endpoint(payload: SqlImportPayload) -> dict[str, Any]:
        result = _service().create_experiment_from_sql(
            name=payload.name,
            sql=payload.sql,
            dialect=payload.dialect,
            target_rows=payload.target_rows,
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
        }

    return app


def _http_status_for_errors(
    result: ExperimentCreateResult | ExperimentDeleteResult | ExperimentGenerateResult,
) -> int:
    """Translate domain errors into HTTP statuses."""

    messages = " ".join(result.errors).lower()
    if "does not exist" in messages:
        return status.HTTP_404_NOT_FOUND
    if "already exists" in messages:
        return status.HTTP_409_CONFLICT
    return status.HTTP_400_BAD_REQUEST


app = create_app()
