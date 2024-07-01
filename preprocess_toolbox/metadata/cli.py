import typer

app = typer.Typer()


@app.command()
def date():
    pass


@app.command()
def mask():
    pass


if __name__ == "__main__":
    app()
