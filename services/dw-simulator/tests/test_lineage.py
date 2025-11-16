"""
Tests for data lineage tracking functionality.

This module tests the lineage tracking system that captures and exposes
foreign key relationships and data provenance through generation runs.
"""

import pytest
from datetime import datetime, timezone

from dw_simulator.lineage import (
    LineageGraph,
    LineageNode,
    LineageEdge,
    export_lineage_dot,
)
from dw_simulator.persistence import ExperimentPersistence
from dw_simulator.schema import parse_experiment_schema


@pytest.fixture
def persistence(tmp_path):
    """Create a temporary persistence instance for testing."""
    db_path = tmp_path / "test_lineage.db"
    return ExperimentPersistence(f"sqlite:///{db_path}")


@pytest.fixture
def sample_schema_with_fks():
    """Sample experiment schema with foreign key relationships."""
    return {
        "name": "ecommerce_lineage",
        "tables": [
            {
                "name": "customers",
                "target_rows": 100,
                "columns": [
                    {"name": "customer_id", "data_type": "INT", "is_unique": True},
                    {"name": "email", "data_type": "VARCHAR", "varchar_length": 255},
                ],
            },
            {
                "name": "orders",
                "target_rows": 500,
                "columns": [
                    {"name": "order_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "customer_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "customers",
                            "references_column": "customer_id",
                        },
                    },
                    {"name": "order_total", "data_type": "FLOAT"},
                ],
            },
            {
                "name": "order_items",
                "target_rows": 2000,
                "columns": [
                    {"name": "item_id", "data_type": "INT", "is_unique": True},
                    {
                        "name": "order_id",
                        "data_type": "INT",
                        "foreign_key": {
                            "references_table": "orders",
                            "references_column": "order_id",
                        },
                    },
                    {"name": "product_name", "data_type": "VARCHAR"},
                ],
            },
        ],
    }


class TestLineageRelationshipsTable:
    """Test lineage_relationships table creation and querying."""

    def test_lineage_table_exists(self, persistence):
        """Verify lineage_relationships table is created automatically."""
        from sqlalchemy import text
        with persistence.engine.connect() as conn:
            result = conn.execute(
                text("SELECT name FROM sqlite_master WHERE type='table' AND name='lineage_relationships'")
            )
            assert result.fetchone() is not None

    def test_store_lineage_relationship(self, persistence, sample_schema_with_fks):
        """Test storing FK relationships in lineage_relationships table."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        # Verify relationships are stored
        relationships = persistence.get_lineage_relationships("ecommerce_lineage")
        assert len(relationships) == 2  # orders->customers, order_items->orders

        # Check first relationship (orders -> customers)
        rel1 = next(r for r in relationships if r.source_table == "orders")
        assert rel1.source_column == "customer_id"
        assert rel1.target_table == "customers"
        assert rel1.target_column == "customer_id"
        assert rel1.relationship_type == "foreign_key"

        # Check second relationship (order_items -> orders)
        rel2 = next(r for r in relationships if r.source_table == "order_items")
        assert rel2.source_column == "order_id"
        assert rel2.target_table == "orders"
        assert rel2.target_column == "order_id"
        assert rel2.relationship_type == "foreign_key"

    def test_lineage_persists_across_reset(self, persistence, sample_schema_with_fks):
        """Verify lineage metadata persists when experiment data is reset."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        # Reset experiment (truncate tables)
        persistence.reset_experiment("ecommerce_lineage")

        # Lineage should still be intact
        relationships = persistence.get_lineage_relationships("ecommerce_lineage")
        assert len(relationships) == 2

    def test_lineage_deleted_with_experiment(self, persistence, sample_schema_with_fks):
        """Verify lineage is removed when experiment is deleted."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        # Verify lineage exists
        relationships = persistence.get_lineage_relationships("ecommerce_lineage")
        assert len(relationships) == 2

        # Delete experiment
        persistence.delete_experiment("ecommerce_lineage")

        # Lineage should be gone
        relationships = persistence.get_lineage_relationships("ecommerce_lineage")
        assert len(relationships) == 0


class TestLineageGraphBuilder:
    """Test in-memory lineage graph construction."""

    def test_build_lineage_graph(self, persistence, sample_schema_with_fks):
        """Test building graph from FK relationships."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        graph = persistence.build_lineage_graph("ecommerce_lineage")

        # Verify nodes (3 tables)
        assert len(graph.nodes) == 3
        assert "customers" in [n.name for n in graph.nodes]
        assert "orders" in [n.name for n in graph.nodes]
        assert "order_items" in [n.name for n in graph.nodes]

        # Verify edges (2 FK relationships)
        assert len(graph.edges) == 2

        # Check edge orders -> customers
        edge1 = next(e for e in graph.edges if e.source.name == "orders")
        assert edge1.target.name == "customers"
        assert edge1.edge_type == "foreign_key"
        assert edge1.metadata["source_column"] == "customer_id"
        assert edge1.metadata["target_column"] == "customer_id"

        # Check edge order_items -> orders
        edge2 = next(e for e in graph.edges if e.source.name == "order_items")
        assert edge2.target.name == "orders"
        assert edge2.edge_type == "foreign_key"

    def test_graph_query_dependencies(self, persistence, sample_schema_with_fks):
        """Test querying table dependencies."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        graph = persistence.build_lineage_graph("ecommerce_lineage")

        # What does orders depend on?
        orders_deps = graph.get_dependencies("orders")
        assert len(orders_deps) == 1
        assert orders_deps[0].name == "customers"

        # What depends on orders?
        orders_dependents = graph.get_dependents("orders")
        assert len(orders_dependents) == 1
        assert orders_dependents[0].name == "order_items"

        # Customers has no dependencies
        customer_deps = graph.get_dependencies("customers")
        assert len(customer_deps) == 0

        # order_items depends on orders (and transitively on customers)
        item_deps = graph.get_all_dependencies("order_items")
        assert len(item_deps) == 2
        assert "orders" in [n.name for n in item_deps]
        assert "customers" in [n.name for n in item_deps]


class TestDotExport:
    """Test GraphViz DOT format export."""

    def test_export_simple_dot(self, persistence, sample_schema_with_fks):
        """Test basic DOT format generation."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        graph = persistence.build_lineage_graph("ecommerce_lineage")
        dot_content = export_lineage_dot(graph, "ecommerce_lineage")

        # Verify DOT structure
        assert "digraph ecommerce_lineage" in dot_content
        assert "customers" in dot_content
        assert "orders" in dot_content
        assert "order_items" in dot_content
        assert "->" in dot_content  # Directed edges

        # Verify FK relationships are represented
        assert "orders -> customers" in dot_content
        assert "order_items -> orders" in dot_content

    def test_dot_includes_column_labels(self, persistence, sample_schema_with_fks):
        """Test that DOT export includes column relationship labels."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        graph = persistence.build_lineage_graph("ecommerce_lineage")
        dot_content = export_lineage_dot(graph, "ecommerce_lineage")

        # Edge labels should show FK column names
        assert "customer_id" in dot_content
        assert "order_id" in dot_content

    def test_dot_valid_syntax(self, persistence, sample_schema_with_fks):
        """Test that generated DOT content has valid syntax."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        graph = persistence.build_lineage_graph("ecommerce_lineage")
        dot_content = export_lineage_dot(graph, "ecommerce_lineage")

        # Basic syntax checks
        assert dot_content.startswith("digraph")
        assert dot_content.count("{") == dot_content.count("}")
        assert dot_content.strip().endswith("}")

        # All nodes should be declared (with row counts)
        assert 'customers [label="customers' in dot_content
        assert 'orders [label="orders' in dot_content
        assert 'order_items [label="order_items' in dot_content
        # Verify row counts are included
        assert "(100 rows)" in dot_content
        assert "(500 rows)" in dot_content
        assert "(2000 rows)" in dot_content

    def test_empty_graph_dot(self, persistence):
        """Test DOT export for experiment with no FK relationships."""
        schema = {
            "name": "simple_experiment",
            "tables": [
                {
                    "name": "standalone_table",
                    "target_rows": 100,
                    "columns": [
                        {"name": "id", "data_type": "INT", "is_unique": True},
                        {"name": "value", "data_type": "VARCHAR"},
                    ],
                }
            ],
        }
        parsed_schema = parse_experiment_schema(schema)
        persistence.create_experiment(parsed_schema)

        graph = persistence.build_lineage_graph("simple_experiment")
        dot_content = export_lineage_dot(graph, "simple_experiment")

        # Should still be valid DOT with one node, no edges
        assert "digraph simple_experiment" in dot_content
        assert "standalone_table" in dot_content
        assert "->" not in dot_content  # No edges


class TestGenerationRunLineage:
    """Test linking data to generation runs for provenance tracking."""

    def test_track_generation_run_in_lineage(self, persistence, sample_schema_with_fks):
        """Test that generation runs are tracked in lineage metadata."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        # Create a generation run
        run_id = persistence.start_generation_run("ecommerce_lineage", seed=42)

        # Verify run_id is valid
        assert run_id > 0

        # Lineage graph should include generation run information
        graph = persistence.build_lineage_graph("ecommerce_lineage")

        # Each node should be able to reference generation runs
        for node in graph.nodes:
            assert hasattr(node, "metadata")
            # After generation, nodes will have generation_run_ids
            # Before generation, the list is empty
            assert "generation_run_ids" in node.metadata
            assert isinstance(node.metadata["generation_run_ids"], list)

    def test_multiple_generation_runs_tracked(self, persistence, sample_schema_with_fks):
        """Test that multiple generation runs are tracked separately."""
        schema = parse_experiment_schema(sample_schema_with_fks)
        persistence.create_experiment(schema)

        # Create multiple runs
        run1 = persistence.start_generation_run("ecommerce_lineage", seed=42)
        persistence.complete_generation_run(run1, row_counts='{"customers": 100}')

        run2 = persistence.start_generation_run("ecommerce_lineage", seed=43)
        persistence.complete_generation_run(run2, row_counts='{"customers": 200}')

        # Verify both runs exist
        runs = persistence.get_generation_runs("ecommerce_lineage")
        assert len(runs) >= 2
        run_ids = [r.id for r in runs]
        assert run1 in run_ids
        assert run2 in run_ids
