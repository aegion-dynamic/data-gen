import random
from typing import Any, Dict, Tuple
import uuid

from faker import Faker
from psycopg import Connection
from data_gen.depgraph import DepGraph, TableNode
from data_gen.sql_enums import (
    check_if_type_is_enum,
    get_enum_options,
    get_user_defined_type,
)


def fill_table(
    table: TableNode, dep_graph: DepGraph, db_connection: Connection, num_rows: int = 10
):
    for column in table.columns:
        print(
            f"Table: {table.full_table_name}, Column: {column.column_name}, Data Type: {column.data_type}"
        )

        for i in range(num_rows):
            print(
                f"Inserting Row {i}: {column.column_name} = {column.data_type}")

            sample_values, sample_types = generate_sample_entry_data(
                table, dep_graph, db_connection
            )

            # TODO: Do the insert into the table

            # Construct the insert statement
            insert_column_names = ""
            insert_column_values = ""

            # Get the column names and values from the sample data
            for column_name, value in sample_values.items():
                # Skip if the column is an id column

                insert_column_names += column_name + ", "
                insert_column_values = _format_append_value(
                    sample_types, column_name, insert_column_values, value
                )

            # Remove the trailing comma and space
            insert_column_names = insert_column_names[:-2]
            insert_column_values = insert_column_values[:-2]

            if insert_column_names == "":
                continue

            # Construct the SQL insert statement
            insert_statement = f"INSERT INTO {table.full_table_name} ( {insert_column_names} ) VALUES ( {insert_column_values} );"
            print("Insert Statement:", insert_statement)

            # Execute the insert statement
            cursor = db_connection.cursor()
            cursor.execute(insert_statement)
            db_connection.commit()


def fill_tables(table_graph: DepGraph, db_connection: Connection):

    # Get the fill order
    fill_order = table_graph.get_fill_order()

    # Fill the tables in the order specified
    for table in fill_order:
        fill_table(table_graph.get_table(table), table_graph, db_connection)


def generate_sample_entry_data(
    table_node: TableNode, dep_graph: DepGraph, db_connection: Connection
) -> Tuple[Dict[str, Any], Dict[str, str]]:
    print("Table Name:", table_node.full_table_name)
    print("Number of parent relationships:",
          len(table_node._parent_relationships))
    fake = Faker()
    dep_values = get_dep_values(table_node, dep_graph, db_connection)

    sample_values = {}
    sample_types = {}

    for column in table_node.columns:
        if column.column_name == "id" and column.data_type == "bigserial":
            continue  # Skip ID generation for bigserial columns
        elif column.column_name == "id":
            # Only set UUID for text IDs
            sample_values["id"] = str(uuid.uuid4())
            sample_types["id"] = "text"

        if column.column_name in dep_values:
            sample_values[column.column_name] = dep_values[column.column_name]
            sample_types[column.column_name] = column.data_type

            print(
                f"Filling column: {column.column_name} with value: {dep_values[column.column_name]} (dependent column)"
            )
            continue

        # if check_if_type_is_enum(column.data_type, db_connection):
        #     enum_options = get_enum_options(column.data_type, db_connection)
        #     sample_values[column.column_name] = random.choice(enum_options)
        #     sample_types[column.column_name] = column.data_type

        #     print(
        #         f"Filling column: {column.column_name} with value: {sample_values[column.column_name]} (enum)"
        #     )
        #     continue

        print(
            f"Filling column: {column.column_name} with type: {column.data_type}")
        sample_types[column.column_name] = column.data_type

        if column.data_type == "bigint":
            sample_values[column.column_name] = random.randint(0, 2**63 - 1)
            sample_types[column.column_name] = "bigint"
        elif column.data_type == "integer":
            sample_values[column.column_name] = random.randint(0, 100)
        elif column.data_type == "text":
            if column.column_name == "first_name":
                sample_values[column.column_name] = fake.first_name()
            elif column.column_name == "last_name":
                sample_values[column.column_name] = fake.last_name()
            elif column.column_name == "email":
                sample_values[column.column_name] = fake.email()
            elif column.column_name == "name":
                sample_values[column.column_name] = fake.word()
            elif column.column_name == "phone_number":
                sample_values[column.column_name] = fake.phone_number()
            elif column.column_name == "id":
                sample_values[column.column_name] = str(uuid.uuid4())
            else:
                sample_values[column.column_name] = fake.sentence()
        elif column.data_type == "date":
            sample_values[column.column_name] = fake.date()
        elif column.data_type == "boolean":
            sample_values[column.column_name] = bool(random.getrandbits(1))
        elif column.data_type == "real":
            sample_values[column.column_name] = random.random()
        elif column.data_type == "jsonb":
            sample_values[column.column_name] = '{"key": "value"}'
        elif (
            column.data_type == "timestamp"
            or column.data_type == "timestamp with time zone"
            or column.data_type == "timestamp without time zone"
        ):
            sample_values[column.column_name] = fake.iso8601()
        elif column.data_type == "USER-DEFINED":
            # TODO: Handle user defined types
            print("WARNING: User defined type not handled")
            user_type = get_user_defined_type(
                schema_name=table_node.schema_name,
                table_name=table_node.table_name,
                column_name=column.column_name,
                conneciton=db_connection
            )
            print(f"User defined type: {user_type}")

            # Get the enum options
            enum_options = get_enum_options(user_type, db_connection)
            sample_values[column.column_name] = random.choice(enum_options)
        elif column.data_type == "numeric":
            sample_values[column.column_name] = random.randint(0, 100)
            sample_types[column.column_name] = "numeric"
        else:
            raise ValueError(
                f"Data type {column.data_type} not supported for column {column.column_name} in table {table_node.full_table_name}")

        print(
            f"Filling column: {column.column_name} with value: {sample_values[column.column_name]}"
        )

    return sample_values, sample_types


def _format_append_value(
    sample_types: Dict[str, str], column_name: str, insert_statement: str, value: Any
) -> str:
    if value is None and column_name != "id":
        insert_statement += "NULL, "
        return insert_statement

    if sample_types[column_name] == "bigint" or sample_types[column_name] == "int":
        insert_statement += f"{value}, "
    elif (
        sample_types[column_name] == "text"
        or sample_types[column_name] == "date"
        or sample_types[column_name] == "jsonb"
    ):
        insert_statement += f"'{value}', "
    elif sample_types[column_name] == "boolean":
        insert_statement += f"{value}, "
    elif sample_types[column_name] == "real":
        insert_statement += f"{value}, "
    else:
        insert_statement += f"'{value}', "

    return insert_statement


def get_dep_values(
    table_node: TableNode, dep_graph: DepGraph, db_connection: Connection
) -> Dict[str, Any]:
    dep_values = {}

    for parent in table_node._parent_relationships:
        dep_values = {}
    for parent in table_node._parent_relationships:
        parent_table_name = parent._parent_table
        parent_column_name = parent._parent_column

        select_query = f"SELECT {parent_column_name} FROM {parent_table_name} ORDER BY RANDOM() LIMIT 1;"
        cursor = db_connection.cursor()
        cursor.execute(select_query)
        row = cursor.fetchone()

        if row is None:
            # Fallback for empty parent tables
            print(f"No rows in {parent_table_name}. Using a default value.")
            dep_values[parent._child_column] = None
            continue

        dep_values[parent._child_column] = row[0]

    return dep_values
