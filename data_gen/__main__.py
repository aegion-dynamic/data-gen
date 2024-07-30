import psycopg

from data_gen.data import fill_tables
from data_gen.depgraph import DepGraph
from data_gen.inspection import generate_dependency_graph


def main():

    # Make the postgres connection
    connection = psycopg.connect(
        "host=localhost dbname=postgres user=postgres password=postgres port=5432"
    )

    # Ping the database
    try:
        connection.cursor()
    except psycopg.OperationalError:
        print("Failed to connect to the database")
        return 1

    # Create DepGraph
    dep_graph = DepGraph()

    # Generate the dependency graph
    generate_dependency_graph(dep_graph, connection)

    # Print all the tables
    print("Tables:")
    all_tables = dep_graph.get_all_tables()
    print(f"Number of tables: {len(all_tables)}")
    for table in all_tables:
        print(table)

    print("\n\n")

    # Print the dependency graph
    print("Dependency Graph:")
    order = dep_graph.get_fill_order()
    print(f"Number of tables: {len(list(order))}")
    for table in order:
        print(table)

    # Draw the graph
    dep_graph.draw_graph()

    dep_graph.print_graph()

    # Fill the tables
    fill_tables(dep_graph, connection)


if __name__ == "__main__":
    main()
