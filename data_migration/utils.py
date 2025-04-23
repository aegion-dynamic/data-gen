from pathlib import Path


def copy_from_templates(file_name: str, output_path:Path) -> None:
    """
    Copy a file from the templates directory to the output path.

    Args:
        file_name (str): The name of the file to copy.
        output_path (Path): The path where the file should be copied to.

    Raises:
        FileNotFoundError: If the template file does not exist.
    """
    # Get the path to the templates directory
    templates_dir = Path(__file__).parent / "default_templates"
    
    # Check if the file exists in the templates directory
    template_file = templates_dir / file_name
    if not template_file.exists():
        raise FileNotFoundError(f"Template file {file_name} not found in {templates_dir}")
    
    # Create the output directory if it doesn't exist
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Copy the file to the output path
    output_path.write_text(template_file.read_text())
