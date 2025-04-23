import click
from pathlib import Path

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
   

    


    



if __name__ == "__main__":
    cli()