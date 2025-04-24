import csv
from pathlib import Path
from typing import Dict, List
from psycopg import Connection
from data_gen.depgraph import DepGraph

def get_csv_data(file_path: Path) -> List[Dict]:
    """
    Read the CSV file and return the data as a list of dictionaries.

    Args:
        file_path (Path): The path to the CSV file.

    Returns:
        List[Dict]: A list of dictionaries representing the rows in the CSV file.
    """
    data = []
    with file_path.open("r") as csv_file:
        # Read the CSV file and convert it to a list of dictionaries
        reader = csv.DictReader(csv_file)
        for row in reader:
            data.append(row)
    return data


def fill_table(table: str, dep_graph: DepGraph, db_connection: Connection):
    """
    Fill the table with data from the CSV file.

    Args:
        table (str): The name of the table to fill.
        dep_graph (DepGraph): The dependency graph.
        db_connection (Connection): The database connection.
    """
    # Get the table node from the dependency graph
    table_node = dep_graph.get_table(table)

    # Get the CSV file path
    csv_file_path = Path(f"table_data/{table_node.schema_name}_{table_node.table_name}.csv")

    table_data = get_csv_data(csv_file_path)

    # Check if the table has any foreign key constraints
    if table_node:
        # If the table has foreign key constraints, we need to fill the parent tables first
        for constraint in table_node.parent_relationships:
            # TODO: find the right lookup column from the relationships.json
            pass



def populate_data(dep_graph: DepGraph, db_connection:Connection):
    
    # Overall process
    # 1. Get the fill order of the tables
    # 2. For each table in the fill order, get the data from the CSV file
    # 3. Insert the data into the table, if no foreign key constraints present and no enum types are present
    # 4. If this is a child of a foreign key constraint, get the data from the parent table and insert it into the child table using the lookup column in the relationships

    # Get the fill order of the tables
    fill_order = dep_graph.get_fill_order()
    
    # Iterate over the tables in the fill order
    for table in fill_order:
        # Get the data from the CSV file
        fill_table(table, dep_graph, db_connection)

    pass