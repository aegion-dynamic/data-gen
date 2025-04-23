from typing import Any, Dict, List

from psycopg import Connection

enum_types_memo: Dict[str, bool] = {}


def check_if_type_is_enum(column_type: str, db_connection: Connection) -> bool:
    if column_type in enum_types_memo:
        return enum_types_memo[column_type]

    query = """
        SELECT DISTINCT
            t.typname AS enum_name  
        FROM pg_type t 
        JOIN pg_enum e ON t.oid = e.enumtypid  
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace;
    """

    cursor = db_connection.cursor()
    cursor.execute(query)

    found = False
    for row in cursor.fetchall():
        if row[0] == column_type:
            found = True
            break

    enum_types_memo[column_type] = found

    return found


enum_values_memo: Dict[str, List[str]] = {}


def get_enum_options(column_type: str, db_connection: Connection) -> List[str]:
    if column_type in enum_values_memo:
        return enum_values_memo[column_type]

    ret = []

    query = """
        SELECT DISTINCT
        e.enumlabel AS enum_value
        FROM pg_type t 
        JOIN pg_enum e ON t.oid = e.enumtypid  
        JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        WHERE t.typname = %s;
    """

    cursor = db_connection.cursor()
    cursor.execute(query, (column_type,))

    for row in cursor.fetchall():
        ret.append(row[0])

    enum_values_memo[column_type] = ret

    return ret


def get_user_defined_type(
    schema_name: str, table_name: str, column_name: str, conneciton: Connection
) -> str:
    
    print(f"Finding USER-DEFINED type for: {schema_name}.{table_name}.{column_name}")
    query = f"""
        SELECT
            udt_name
        FROM
            information_schema.columns
        WHERE
            table_schema = '{schema_name}'
            AND table_name = '{table_name}'
            AND column_name = '{column_name}';
    """

    cursor = conneciton.cursor()
    cursor.execute(query)

    if cursor is None:
        raise ValueError(f"User defined type not found for: {schema_name}.{table_name}.{column_name}")
    
    ret = cursor.fetchone()[0]

    if ret is None:
        raise ValueError(f"User defined type not found for: {schema_name}.{table_name}.{column_name}")

    return ret
