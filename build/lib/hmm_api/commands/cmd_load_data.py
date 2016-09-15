import click
from hmm_api.cli import global_test_options, etl_test_options
from hmm_api.etl import load_data

@click.command('load_data', short_help='Load data into the database')
#@global_test_options
@etl_test_options
@click.option('--hmm', default=None, multiple=True, type=click.Path(exists=True), help='Add hmm files to database')
def cli(sample, hmm):
    """Create the database."""

    click.echo("Create the database")
    hmm = load_data.HmmSearch()
    hmm.iter_results()
    click.echo("finished with no errors!")
