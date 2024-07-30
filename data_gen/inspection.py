from typing import Any, Dict

from psycopg import Connection

from data_gen.depgraph import DepGraph
from data_gen.table_node import TableNode


def generate_dependency_graph(dep_graph: DepGraph, db_connection: Connection):
    # Query to retrieve schema information
    query = """
        SELECT table_schema, table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    cursor = db_connection.cursor()
    cursor.execute(query)

    # Iterate over the rows and print table names, column names and their types
    print("Tables and Columns:")
    rows = cursor.fetchall()
    print(f"Total Number of table rows: {len(rows)}")

    table_objects_memo: Dict[str, TableNode] = (
        {}
    )  # Store them here first before adding into dep graph

    for row in rows:
        schema, table, column, data_type = row
        print(f"{schema}.{table}.{column}: {data_type}")

        # Create the full table name (use this only for any logic)
        full_table_name = f"{schema}.{table}"

        table_object = None

        # Create a new TableNode if it doesn't exist
        if full_table_name in table_objects_memo:
            table_object = table_objects_memo[full_table_name]
        else:
            table_object = TableNode(full_table_name=full_table_name)
            table_objects_memo[full_table_name] = table_object

        # Add the column to the TableNode corresponding to the table
        table_object.add_column(column_name=column, data_type=data_type)

    # Store the table object in the memo onto the dep graph
    for _, table_object in table_objects_memo.items():
        dep_graph.add_table(table_object)

    # Query to retrieve foreign key information
    query = """
        SELECT
            tc.table_schema, tc.table_name, kcu.column_name, tc.constraint_name,
            ccu.table_schema AS foreign_table_schema,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY';    
    """
    cursor.execute(query)

    # Iterate over the rows and print foreign key information
    print("\nForeign Keys:")
    for row in cursor.fetchall():
        (
            schema,
            table,
            column,
            constraint_name,
            foreign_schema,
            foreign_table,
            foreign_column,
        ) = row
        print(
            f"{constraint_name} : {schema}.{table}.{column} -> {foreign_schema}.{foreign_table}.{foreign_column}"
        )

        # Create the full table name (current)
        current_table_name = f"{schema}.{table}"
        current_table_object = dep_graph.get_table(current_table_name)

        # Create the full table name (foreign) and retrieve the TableNode
        foreign_table_name = f"{foreign_schema}.{foreign_table}"

        foreign_table_object = dep_graph.get_table(foreign_table_name)

        # Add the foreign table as a child of the table
        # table_nodes[foreign_table_name].add_child(table_nodes[current_table_name], constraint_name, foreign_column, column)
        dep_graph.add_child(
            child=current_table_object,
            parent=foreign_table_object,
            constraint_name=constraint_name,
            parent_column=foreign_column,
            child_column=column,
        )
