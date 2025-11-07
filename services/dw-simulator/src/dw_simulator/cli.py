"""
Command-line interface for the DW Simulator containerized workflow.

The CLI exposes operational helpers that run inside the synthetic-data-generator
container defined in docker-compose.yml. It keeps the app aligned with the
local-first architecture from docs/tech-spec.md by surfacing runtime metadata
and container health checks.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import typer
import uvicorn

from . import __version__
from .config import get_stage_bucket, get_target_db_url
from .service import (
    ExperimentCreateResult,
    ExperimentDeleteResult,
    ExperimentResetResult,
    ExperimentGenerateResult,
    QueryExecutionResult,
    ExperimentService,
    SUPPORTED_DIALECTS,
)

app = typer.Typer(help="DW Simulator utility that powers the synthetic-data-generator service.")
experiment_app = typer.Typer(help="Manage experiment schemas and lifecycle.")
query_app = typer.Typer(help="Execute SQL queries and export results.")
app.add_typer(experiment_app, name="experiment")
app.add_typer(query_app, name="query")


@dataclass(frozen=True)
class RuntimeMetadata:
    """Represents the runtime configuration surfaced via the CLI."""

    target_db_url: str
    stage_bucket: str

    @classmethod
    def from_environ(cls) -> "RuntimeMetadata":
        """Load metadata from environment variables with sensible defaults."""
        return cls(target_db_url=get_target_db_url(), stage_bucket=get_stage_bucket())


@app.command()
def doctor() -> None:
    """
    Emit JSON describing the container runtime configuration.

    Docker Compose uses this command as the default entrypoint so operators can
    quickly confirm wiring between the Python utility, SQLite (Redshift mock),
    and LocalStack services before triggering heavier workflows.
    """

    metadata = RuntimeMetadata.from_environ()
    typer.echo(
        json.dumps(
            {
                "component": "synthetic-data-generator",
                "version": __version__,
                "target_db_url": metadata.target_db_url,
                "stage_bucket": metadata.stage_bucket,
            },
            indent=2,
        )
    )


@app.command()
def version() -> None:
    """Print the DW Simulator version (mirrors `dw-sim version`)."""

    typer.echo(__version__)


@app.command("api")
def run_api(
    host: str = typer.Option("0.0.0.0", help="Bind host for the FastAPI server."),
    port: int = typer.Option(8000, help="Port for the FastAPI server."),
    reload: bool = typer.Option(False, help="Enable auto-reload (dev only)."),
) -> None:
    """Run the FastAPI control plane."""

    uvicorn.run("dw_simulator.api:app", host=host, port=port, reload=reload)  # pragma: no cover


@experiment_app.command("create")
def create_experiment(schema_file: Path = typer.Argument(..., help="Path to the experiment JSON schema.")) -> None:
    """
    Create a new experiment by reading the provided JSON schema file.
    """

    service = ExperimentService()
    result = service.create_experiment_from_file(schema_file)
    if not result.success:
        _print_errors_and_exit(result)

    assert result.metadata is not None  # For type checkers; success implies metadata
    typer.secho(
        f"Experiment '{result.metadata.name}' created with {len(result.metadata.schema_json)} bytes of schema.",
        fg=typer.colors.GREEN,
    )


@experiment_app.command("delete")
def delete_experiment(name: str = typer.Argument(..., help="Name of the experiment to delete.")) -> None:
    """Delete an experiment's metadata and physical tables."""

    service = ExperimentService()
    result = service.delete_experiment(name)
    if not result.success:
        _print_errors_and_exit(result)

    typer.secho(
        f"Experiment '{name}' deleted (dropped {result.deleted_tables} tables).",
        fg=typer.colors.GREEN,
    )


@experiment_app.command("reset")
def reset_experiment(name: str = typer.Argument(..., help="Name of the experiment to reset.")) -> None:
    """Reset an experiment by truncating all tables without deleting the schema."""

    service = ExperimentService()
    result = service.reset_experiment(name)
    if not result.success:
        _print_errors_and_exit(result)

    typer.secho(
        f"Experiment '{name}' reset (truncated {result.reset_tables} tables).",
        fg=typer.colors.GREEN,
    )


@experiment_app.command("generate")
def generate_experiment(
    name: str = typer.Argument(..., help="Experiment name to generate data for."),
    rows: list[str] = typer.Option(None, "--rows", "-r", help="Override target rows via table=value (can repeat)."),
    seed: int | None = typer.Option(None, help="Optional RNG seed for deterministic generation."),
    output_dir: Path | None = typer.Option(None, help="Override output directory for generated Parquet files."),
) -> None:
    """Generate synthetic data for an experiment."""

    overrides: dict[str, int] = {}
    for entry in rows or []:
        if "=" not in entry:
            raise typer.BadParameter("Row overrides must use the form table=rows.")
        table, value = entry.split("=", 1)
        overrides[table.strip()] = int(value)

    service = ExperimentService()
    result = service.generate_data(experiment_name=name, rows=overrides, seed=seed, output_dir=output_dir)
    if not result.success or result.summary is None:
        _print_errors_and_exit(result)

    typer.secho(
        f"Generated data for experiment '{name}' into {result.summary.output_dir}",
        fg=typer.colors.GREEN,
    )
    for table in result.summary.tables:
        typer.echo(f" - {table.table_name}: {table.row_count} rows across {len(table.files)} file(s)")


@experiment_app.command("import-sql")
def import_sql_command(
    sql_file: Path = typer.Argument(..., help="Path to the SQL file containing CREATE TABLE statements."),
    name: str = typer.Option(..., "--name", "-n", help="Experiment name to store in the simulator."),
    dialect: str = typer.Option("redshift", "--dialect", "-d", help=f"Dialect ({', '.join(SUPPORTED_DIALECTS)})"),
    target_rows: int = typer.Option(1000, help="Default target row count per table."),
) -> None:
    """Create an experiment by importing warehouse DDL (Redshift/Snowflake)."""

    if dialect.lower() not in SUPPORTED_DIALECTS:
        typer.secho(f"Unsupported dialect '{dialect}'. Choose from {', '.join(SUPPORTED_DIALECTS)}.", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)
    try:
        sql_text = sql_file.read_text()
    except OSError as exc:
        typer.secho(f"Failed to read SQL file: {exc}", err=True, fg=typer.colors.RED)
        raise typer.Exit(code=1)

    service = ExperimentService()
    result = service.create_experiment_from_sql(name=name, sql=sql_text, dialect=dialect, target_rows=target_rows)
    if not result.success:
        _print_errors_and_exit(result)
    assert result.metadata
    typer.secho(f"Experiment '{name}' created from SQL ({dialect})", fg=typer.colors.GREEN)


@query_app.command("execute")
def execute_query(
    sql: str = typer.Argument(..., help="SQL query to execute."),
    output: Path | None = typer.Option(None, "--output", "-o", help="Export results to CSV file."),
) -> None:
    """
    Execute a SQL query against the local warehouse.

    Examples:
        dw-sim query execute "SELECT * FROM my_experiment__customers LIMIT 10"
        dw-sim query execute "SELECT * FROM my_experiment__orders" --output results.csv
    """
    service = ExperimentService()
    result = service.execute_query(sql)

    if not result.success or not result.result:
        _print_query_errors_and_exit(result)

    # Export to CSV if output file specified
    if output:
        csv_content = service.export_query_results_to_csv(result.result)
        output.write_text(csv_content)
        typer.secho(f"Query results exported to {output} ({result.result.row_count} rows)", fg=typer.colors.GREEN)
    else:
        # Print results to console
        typer.secho(f"Query returned {result.result.row_count} rows:", fg=typer.colors.GREEN)

        # Print column headers
        if result.result.columns:
            typer.echo(" | ".join(result.result.columns))
            typer.echo("-" * (sum(len(col) for col in result.result.columns) + 3 * (len(result.result.columns) - 1)))

        # Print rows (limit to first 50 for console display)
        for i, row in enumerate(result.result.rows):
            if i >= 50:
                typer.echo(f"... ({result.result.row_count - 50} more rows)")
                break
            typer.echo(" | ".join(str(val) for val in row))


@query_app.command("save")
def save_query(
    sql: str = typer.Argument(..., help="SQL query text to save."),
    output: Path = typer.Option(..., "--output", "-o", help="Output .sql file path."),
) -> None:
    """
    Save a SQL query to a .sql file.

    Examples:
        dw-sim query save "SELECT * FROM my_experiment__customers" --output query.sql
    """
    service = ExperimentService()
    service.save_query_to_file(sql, output)
    typer.secho(f"Query saved to {output}", fg=typer.colors.GREEN)


def _print_errors_and_exit(result: ExperimentDeleteResult | ExperimentCreateResult | ExperimentResetResult | ExperimentGenerateResult) -> None:
    for error in result.errors:
        typer.secho(error, err=True, fg=typer.colors.RED)
    raise typer.Exit(code=1)


def _print_query_errors_and_exit(result: QueryExecutionResult) -> None:
    for error in result.errors:
        typer.secho(error, err=True, fg=typer.colors.RED)
    raise typer.Exit(code=1)


if __name__ == "__main__":  # pragma: no cover
    app()
