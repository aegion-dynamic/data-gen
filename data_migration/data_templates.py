from pathlib import Path
from psycopg import Connection

from data_gen.depgraph import DepGraph

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