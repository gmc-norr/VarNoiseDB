import click
import logging
from varnoisedb.database import DatabaseAdapter
from varnoisedb.updater import Updater
from varnoisedb.gvcf_parser import GVCFParser

logging.basicConfig(level=logging.INFO)

@click.command()
@click.option('--gvcf', required=True, type=click.Path(exists=True), help='Path to the .gvcf file')
@click.option('--batch-size', default=1000, help='Number of variants to process in each batch')
@click.pass_context
def load(ctx, gvcf, batch_size):
    """Load data from a .gvcf file into the database."""
    config = ctx.obj['CONFIG']
    db_config = config['database']
    
    logging.info(f"Loading data from {gvcf} into the database with batch size {batch_size}...")
    
    db_adapter = DatabaseAdapter(
        db_type=db_config['type'],
        db_name=db_config['name'],
        host=db_config.get('host'),
        port=db_config.get('port'),
        user=db_config.get('user'),
        password=db_config.get('password')
    )
    gvcf_parser = GVCFParser(gvcf)
    updater = Updater(db_adapter, gvcf_parser, batch_size)
    
    updater.update_database()
    
    db_adapter.close()
    logging.info("Data loading complete.")
