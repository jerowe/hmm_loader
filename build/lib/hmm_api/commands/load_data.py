import click

from hmm_api.cli import global_test_options

@click.command('load_data', short_help='Load data into the database')
@global_test_options
def cli(verbose):
    """Create the database."""

    click.echo("Create the database")
    pass
