import click
from hmm_api.cli import global_options


@click.command('create_db', short_help='Create Database')
@global_options
def cli(verbose):
    """Create the database."""

    click.echo("Create the database")
    pass
