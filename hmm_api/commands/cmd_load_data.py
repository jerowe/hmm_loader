import click
#from hmm_api.cli import global_options, etl_options
from hmm_api.cli import etl_options
from hmm_api.etl import load_data

@click.command('load_data', short_help='Load data into the database')
#@global_options
@etl_options
@click.option('--hmm_file', default=None, multiple=True, required = True, type=click.Path(exists=True), help='Add hmm files to database')
def cli(sample, dbfile, hmm_file):
    """Load an hmm file into the database."""

    click.echo("Beginning load - this may take some time")

    if dbfile:
        hmm = load_data.HmmSearch(sample_name = sample, search_files = hmm_file, db_file = dbfile)
    else:
        hmm = load_data.HmmSearch(sample_name = sample, search_files = hmm_file)

    hmm.iter_results()

    click.echo("finished with no errors!")
