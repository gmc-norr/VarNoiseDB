import os
import click
import logging
import yaml
import importlib.resources as pkg_resources
from varnoisedb.schema import validate_config
from varnoisedb.__version__ import __version__

from varnoisedb.cli.load import load
from varnoisedb.cli.export import export

logging.basicConfig(level=logging.INFO)

def load_config(config_file):
    with open(config_file, 'r') as file:
        return yaml.safe_load(file)

@click.group()
@click.option('--config', type=click.Path(exists=True), help='Path to the configuration file')
@click.version_option(__version__)
@click.pass_context
def cli(ctx, config):
    if config:
        config_file = config
    elif os.getenv('VARNOISEDB_CONFIG'):
        config_file = os.getenv('VARNOISEDB_CONFIG')
    else:
        with pkg_resources.path('varnoisedb', 'default_config.yaml') as default_config_path:
            config_file = str(default_config_path)
    
    ctx.ensure_object(dict)
    config = load_config(config_file)
    config = validate_config(config)
    ctx.obj['CONFIG'] = config

    db_config = config['database']
    db_type = db_config['type']
    db_host = db_config.get('host', 'localhost')
    db_port = db_config.get('port', 5432 if db_type == 'postgresql' else 3306)
    
    logging.info(f"Connecting to {db_type} database at {db_host}:{db_port} using database name {db_config['name']}")

cli.add_command(load)
cli.add_command(export)

if __name__ == "__main__":
    cli(obj={})
