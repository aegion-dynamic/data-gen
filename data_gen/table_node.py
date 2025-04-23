from typing import List


class ForeignKeyConstraint:

    def __init__(
        self,
        constraint_name: str,
        parent_column: str,
        parent_table: str,
        child_table: str,
        child_column: str,
    ):
        self._constraint_name = constraint_name
        self._parent_column = parent_column
        self._parent_table = parent_table
        self._child_table = child_table
        self._child_column = child_column

    @property
    def constraint_name(self) -> str:
        return self._constraint_name

    @property
    def parent_column(self) -> str:
        return self._parent_column

    @property
    def parent_table(self) -> str:
        return self._parent_table

    @property
    def child_table(self) -> str:
        return self._child_table

    def __str__(self):
        return f"{self._constraint_name}: ({self._parent_table}.{self._parent_column} -> {self._child_table}.{self._child_column})"


class TableColumn:

    def __init__(self, column_name: str, data_type: str):
        self._column_name = column_name
        self._data_type = data_type

    @property
    def column_name(self) -> str:
        return self._column_name

    @property
    def data_type(self) -> str:
        return self._data_type

    def __str__(self):
        return f"{self._column_name}: {self._data_type}"


class TableNode:

    def __init__(
        self,
        full_table_name: str,
    ):
        self._full_table_name = full_table_name
        self._columns: List[TableColumn] = []
        self._parent_relationships: List[ForeignKeyConstraint] = []

    @property
    def full_table_name(self) -> str:
        return self._full_table_name
    
    @property
    def table_name(self) -> str:
        return self._full_table_name.split(".")[1]
    
    @property
    def schema_name(self) -> str:
        return self._full_table_name.split(".")[0]
    
    @property
    def columns(self) -> List[TableColumn]:
        return self._columns

    @property
    def parent_relationships(self) -> List[ForeignKeyConstraint]:
        return self._parent_relationships

    def add_column(self, column_name: str, data_type: str):
        print(f"Adding column: {column_name} to table: {self._full_table_name}")
        self._columns.append(TableColumn(column_name=column_name, data_type=data_type))

    def sdd_parent_relationship(self, parent_relationship: ForeignKeyConstraint):
        self._parent_relationships.append(parent_relationship)

    def get_csv_header(self) -> str:
        """
        Get the CSV header for the table.
        """
        return ",".join([col.column_name for col in self._columns])

    def __str__(self):
        ret = f"Table: {self._full_table_name}\n"
        ret += (
            f"Columns: ({len(self.columns)}) \n"
            + "\n".join([str(col) for col in self._columns])
            + "\n"
        )
        ret += (
            "Parent Relationships: \n"
            + "\n".join([str(rel) for rel in self._parent_relationships])
            + "\n"
        )
        return ret
