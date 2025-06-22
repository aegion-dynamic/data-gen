import pathlib
from typing import Dict, List
import networkx as nx
import os

from data_gen.table_node import ForeignKeyConstraint, TableNode


class DepGraph(nx.DiGraph):

    def __init__(self):
        super(DepGraph, self).__init__()
        self._tables: Dict[str, TableNode] = {}

    def get_all_tables(self) -> List[TableNode]:
        return list(self._tables.values())

    def add_table(self, table_node: TableNode):
        self._tables[table_node.full_table_name] = table_node
        self.add_node(table_node.full_table_name)

    def get_table(self, table_name: str) -> TableNode:
        if table_name not in self._tables:
            raise ValueError(f"Table {table_name} not found in graph")

        return self._tables[table_name]

    def add_child(
        self,
        child: TableNode,
        parent: TableNode,
        constraint_name: str,
        parent_column: str,
        child_column: str,
    ):

        # Add the foreign key constraint to the parent and child nodes
        child.sdd_parent_relationship(
            ForeignKeyConstraint(
                constraint_name=constraint_name,
                parent_table=parent.full_table_name,
                parent_column=parent_column,
                child_table=child.full_table_name,
                child_column=child_column,
            )
        )

        # Add to networkx graph
        self.add_edge(parent.full_table_name, child.full_table_name)

    def get_fill_order(self) -> List[str]:

        # Check if its a DAG
        if not nx.is_directed_acyclic_graph(self):
            raise ValueError("Graph is not a directed acyclic graph")

        # Get the topological sort
        order = list(nx.topological_sort(self))

        # Since this doesn't include the nodes without any edges, we need to add them
        for node in self.nodes:
            if node not in order:
                order.append(node)

        return order

    def print_graph(self):

        for edge in self.edges():
            print(f"{edge[0]} -> {edge[1]}")
            print("\n")

    def draw_graph(self, filename: str = "depgraph") -> None:
        tt = pathlib.Path("./").joinpath(filename + ".dot")
        print("output:", str(tt.absolute()))
        nx.nx_agraph.to_agraph(self).write(str(tt.absolute()))

        os.system(
            "dot -Tpdf {} -o {}.pdf".format(
                str(tt.absolute()), pathlib.Path("./").joinpath(tt.stem)
            )
        )
