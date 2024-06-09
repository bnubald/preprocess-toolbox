"""Console script for preprocess_toolbox."""
import preprocess_toolbox

import typer
from rich.console import Console

app = typer.Typer()
console = Console()


@app.command()
def main():
    """Console script for preprocess_toolbox."""
    console.print("Replace this message by putting your code into "
               "preprocess_toolbox.cli.main")
    console.print("See Typer documentation at https://typer.tiangolo.com/")
    


if __name__ == "__main__":
    app()
