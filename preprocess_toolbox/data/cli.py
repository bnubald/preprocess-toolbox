import typer

app = typer.Typer()


@app.command()
def amsr2():
    pass


@app.command()
def cmip6():
    pass


@app.command()
def era5():
    pass


@app.command()
def osisaf():
    pass


if __name__ == "__main__":
    app()
