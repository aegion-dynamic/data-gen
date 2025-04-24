import json
from pathlib import Path
from typing import List, Optional
from psycopg import Connection
from pydantic import BaseModel, Field

from data_gen.depgraph import DepGraph


class Relationship(BaseModel):
    name: str = Field(..., description="The name of the relationship.")
    description: str = Field(..., description="The description of the relationship.")
    source_field_schema: str = Field(..., description="The source field of the relationship.")
    target_field_schema: str = Field(..., description="The target field of the relationship.")
    source_field_table: str = Field(..., description="The source table of the relationship.")
    target_field_table: str = Field(..., description="The target table of the relationship.")
    source_field_column: str = Field(..., description="The source column of the relationship.")
    target_field_column: str = Field(..., description="The target column of the relationship.")
    target_field_lookup_column: Optional[str] = Field(default="", description="The target column of the relationship.")


def print_fill_order(dep_graph: DepGraph):
    
    order = dep_graph.get_fill_order()

    # Write the fill order in `fill_order.txt`
    with open("fill_order.txt", "w") as f:
        for table in order:
            f.write(f"{table}\n")


def generate_date_templates(dep_graph: DepGraph, output_path: Path):
    """
    Generate data templates for each table in the dependency graph.

    Args:
        dep_graph (DepGraph): The dependency graph.
        output_path (Path): The path where the data templates should be saved.
    """
    # Create the output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)

    # Iterate over all tables in the dependency graph
    for table_node in dep_graph.get_all_tables():
        # Generate the data template for the table
        template_file = output_path / f"{table_node.schema_name}_{table_node.table_name}.sql"
        csv_header = table_node.get_csv_header()
        with open(template_file, "w") as f:
            # Write the CSV header to the file
            f.write(f"{csv_header}\n")

        print(f"Generated data template for {table_node.full_table_name} at {template_file}")


def generate_relationships(dep_graph: DepGraph, output_path: Path):
    """
    Generate relationships for each table in the dependency graph.

    Args:
        dep_graph (DepGraph): The dependency graph.
        output_path (Path): The path where the relationships should be saved.
    """
   # First run through each of the table nodes in the dep graph and construct the relationships
    relationships:List[Relationship] = []
    for table_node in dep_graph.get_all_tables():
        for parent_relationship in table_node.parent_relationships:

            source_node = dep_graph.get_table(parent_relationship.child_table)
            target_node = dep_graph.get_table(parent_relationship.parent_table)

            relationships.append(
                Relationship(
                    name=parent_relationship.constraint_name,
                    description="",
                    source_field_schema=source_node.schema_name,
                    target_field_schema=target_node.schema_name,
                    source_field_table=source_node.table_name,
                    target_field_table=target_node.table_name,
                    source_field_column=parent_relationship.child_column,
                    target_field_column=parent_relationship.parent_column,
                    target_field_lookup_column="Fill Me !",
                )
            )

    # Write the relationships to a JSONL file
    with open(output_path, "w") as f:
        for relationship in relationships:
            f.write(relationship.model_dump_json() + "\n")

