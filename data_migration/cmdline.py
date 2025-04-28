import json
import click
from pathlib import Path

import psycopg

from data_gen.depgraph import DepGraph
from data_gen.inspection import generate_dependency_graph
from data_migration.users import CognitoConfig, UserModel, create_cognito_users, update_cognito_users
from data_migration.data_templates import generate_date_templates, generate_relationships, print_fill_order
from data_migration.utils import copy_from_templates

@click.group()
def cli():
    """Command line interface for data migration."""
    pass

@cli.command()
@click.option('--target-dir', required=False,type=Path, default=Path("."), help='Target directory for data migration.')
def init_dir(target_dir):
    print("Creating the directory structure for data migration.")
    # Create the target directory if it doesn't exist
    target_dir = Path(target_dir)
    schema_dir: Path = target_dir / "schema"
    users_dir: Path = target_dir / "users"
    table_data_dir: Path = target_dir / "table_data"

    # Create subdirectories and the default files
    target_dir.mkdir(parents=True, exist_ok=True)
    
    # Create the schema subdirectory
    schema_dir.mkdir(parents=True, exist_ok=True)

    # Create the users subdirectory
    users_dir.mkdir(parents=True, exist_ok=True)
    copy_from_templates("users.csv",(users_dir / "users.csv"))
    copy_from_templates("congnito_config.json", (users_dir / "cognito_config.json"))

    # Create the table_data subdirectory
    table_data_dir.mkdir(parents=True, exist_ok=True)
    copy_from_templates("relationships.json",  (table_data_dir / "relationships.json"))

    # Copy the makefile from default_template
    makefile_path = "Makefile"
    copy_from_templates(file_name=makefile_path, output_path=target_dir / "Makefile")

@cli.command()
def generate_users():

    # Default cognito config file path
    cognito_config_path = Path("users/cognito_config.json")
    users_path = Path("users/users.csv")


    # Load the cognito config file
    if not cognito_config_path.exists():
        raise FileNotFoundError(f"Cognito config file {cognito_config_path} not found.")
    
    # Load the cognito config file
    with open(cognito_config_path, 'r') as f:
        cognito_config_data = json.load(f)

    cognito_config = CognitoConfig(**cognito_config_data)
    
    # Get users from csv 
    users = UserModel.from_csv(users_path)
    
    # Create cognito users
    create_cognito_users(users, cognito_config)
    print("Cognito users creation completed.")
    update_cognito_users(users, users_path)
    print("Cognito users updated in the CSV file.")

@cli.command()
def generate_data_templates():
    # Connect to the database and generate the dependency graph

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

    # Generate the data templates
    generate_date_templates(dep_graph, (Path("table_data")))

    print("Data templates generated successfully.")
    # Generate the relationships
    generate_relationships(dep_graph, (Path("table_data/relationships.json")))
    
    # Generate the dependency order
    print_fill_order(dep_graph)
    print("Data templates fill order generated successfully.")
    

@cli.command()
def populate_db():
    # Connect to the database and generate the dependency graph
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

    # Populate the database with data templates
    pass

if __name__ == "__main__":
    cli()