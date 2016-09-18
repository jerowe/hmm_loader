import click
from hmm_api.cli import global_options, etl_options
from hmm_api.etl import load_data

@click.command('load_data', short_help='Load data into the database')
#@global_options
@etl_options
@click.option('--hmm_file', default=None, multiple=True, required = True, type=click.Path(exists=True), help='Add hmm files to database')
@click.option('--db_drive', type=click.Choice(['sql', 'orm']), help='Choose a database driver type. Options are sqlite3 (sql) or sqlalchemy (orm)')
def cli(sample, dbfile, hmm_file, db_drive):
    """Load an hmm file into the database."""

    click.echo("Beginning load - this may take some time")

    if dbfile:
        hmm = load_data.HmmSearch(sample_name = sample, search_files = hmm_file, db_file = dbfile, db_drive = db_drive)
    else:
        hmm = load_data.HmmSearch(sample_name = sample, search_files = hmm_file)

    hmm.iter_results()

    click.echo("finished with no errors!")
