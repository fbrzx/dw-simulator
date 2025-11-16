"""
Data lineage tracking and visualization module.

This module provides functionality for:
- Tracking foreign key relationships between tables
- Building in-memory lineage graphs
- Exporting lineage to GraphViz DOT format
- Querying data provenance through generation runs
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class LineageRelationship:
    """Represents a stored lineage relationship (typically a foreign key)."""

    id: int
    experiment_name: str
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    relationship_type: str  # "foreign_key", "derived", etc.


@dataclass
class LineageNode:
    """Represents a table or entity in the lineage graph."""

    name: str
    node_type: str = "table"  # "table", "column", "view"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if not isinstance(other, LineageNode):
            return False
        return self.name == other.name


@dataclass
class LineageEdge:
    """Represents a relationship between two nodes in the lineage graph."""

    source: LineageNode
    target: LineageNode
    edge_type: str  # "foreign_key", "derived_from", etc.
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageGraph:
    """In-memory representation of a lineage graph for an experiment."""

    experiment_name: str
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)

    def get_node(self, name: str) -> LineageNode | None:
        """Get a node by name."""
        for node in self.nodes:
            if node.name == name:
                return node
        return None

    def get_dependencies(self, table_name: str) -> list[LineageNode]:
        """Get direct dependencies (tables this table depends on via FKs)."""
        dependencies = []
        for edge in self.edges:
            if edge.source.name == table_name:
                dependencies.append(edge.target)
        return dependencies

    def get_dependents(self, table_name: str) -> list[LineageNode]:
        """Get direct dependents (tables that depend on this table)."""
        dependents = []
        for edge in self.edges:
            if edge.target.name == table_name:
                dependents.append(edge.source)
        return dependents

    def get_all_dependencies(self, table_name: str, visited: set[str] | None = None) -> list[LineageNode]:
        """Get all transitive dependencies recursively."""
        if visited is None:
            visited = set()

        if table_name in visited:
            return []

        visited.add(table_name)
        all_deps = []

        direct_deps = self.get_dependencies(table_name)
        for dep in direct_deps:
            if dep.name not in visited:
                all_deps.append(dep)
                # Recursively get dependencies of dependencies
                transitive = self.get_all_dependencies(dep.name, visited)
                all_deps.extend(transitive)

        return all_deps

    def to_dict(self) -> dict[str, Any]:
        """Convert graph to JSON-serializable dictionary."""
        return {
            "experiment_name": self.experiment_name,
            "nodes": [
                {
                    "name": node.name,
                    "type": node.node_type,
                    "metadata": node.metadata,
                }
                for node in self.nodes
            ],
            "edges": [
                {
                    "source": edge.source.name,
                    "target": edge.target.name,
                    "type": edge.edge_type,
                    "metadata": edge.metadata,
                }
                for edge in self.edges
            ],
        }


def export_lineage_dot(graph: LineageGraph, title: str | None = None) -> str:
    """
    Export lineage graph to GraphViz DOT format.

    Args:
        graph: LineageGraph to export
        title: Optional title for the graph (defaults to experiment name)

    Returns:
        String containing valid DOT format syntax
    """
    title = title or graph.experiment_name
    # Sanitize title for DOT (replace spaces/special chars with underscores)
    graph_id = title.replace(" ", "_").replace("-", "_")

    lines = [f"digraph {graph_id} {{"]
    lines.append(f'  label="{title}";')
    lines.append("  rankdir=LR;")  # Left-to-right layout
    lines.append("  node [shape=box, style=filled, fillcolor=lightblue];")
    lines.append("")

    # Add nodes
    for node in graph.nodes:
        node_id = node.name.replace(" ", "_")
        label = node.name

        # Add row count to label if available
        if "target_rows" in node.metadata:
            label += f"\\n({node.metadata['target_rows']} rows)"

        lines.append(f'  {node_id} [label="{label}"];')

    lines.append("")

    # Add edges
    for edge in graph.edges:
        source_id = edge.source.name.replace(" ", "_")
        target_id = edge.target.name.replace(" ", "_")

        # Create edge label from metadata
        label_parts = []
        if "source_column" in edge.metadata and "target_column" in edge.metadata:
            label_parts.append(f"{edge.metadata['source_column']} → {edge.metadata['target_column']}")

        if label_parts:
            label = "\\n".join(label_parts)
            lines.append(f'  {source_id} -> {target_id} [label="{label}"];')
        else:
            lines.append(f'  {source_id} -> {target_id};')

    lines.append("}")

    return "\n".join(lines)


__all__ = [
    "LineageRelationship",
    "LineageNode",
    "LineageEdge",
    "LineageGraph",
    "export_lineage_dot",
]
