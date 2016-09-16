import click
import os
import sys

class MyCLI(click.MultiCommand):

    #this should be a list
    @property
    def plugin_folder(self):
        return os.path.join(os.path.dirname(__file__), 'commands')

    def list_commands(self, ctx):
        rv = []
        for filename in os.listdir(self.plugin_folder):
            if filename.endswith('.py') and \
                    filename.startswith('cmd_'):
                        rv.append(filename[4:-3])
        rv.sort()
        # click.echo("RVS are {}".format(rv))
        return rv

    def get_command(self, ctx, name):
        try:
            if sys.version_info[0] == 2:
                name = name.encode('ascii', 'replace')
            mod = __import__('hmm_api.commands.cmd_' + name,
                    None, None, ['cli'])
        except ImportError as e:
            click.echo("we got an import error on {}".format(name))
            click.echo("Error is {}".format(str(e)))
            return
        return mod.cli

class GlobalOpts(object):

    def __init__(self):
        self.global_values = [
                click.option('--verbose', '-v',  is_flag = True, default=False, help='Enable verbose logging'),
                ]
        self.etl_values = [
                click.option('--sample', '-s', default=None,  help='Sample to query for'),
                click.option('--dbfile', '-db', default=None, type=click.Path(exists=False), required=False,  help='Path to sqlitedb'),
                ]

    def global_options(self, func):
        for option in self.global_values:
            func = option(func)
        return func

    def etl_options(self, func):
        for option in self.etl_values:
            func = option(func)
        return func

global_opts = GlobalOpts()
global_options = global_opts.global_options
etl_options = global_opts.etl_options

@click.command(cls=MyCLI)
def cli():
    """ HMM Database ETL """
    pass
