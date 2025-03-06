import click
import logging
import os
from varnoisedb.database import DatabaseAdapter
from varnoisedb.updater import Updater
from varnoisedb.gvcf_parser import GVCFParser
from varnoisedb.models import Sample

logging.basicConfig(level=logging.INFO)

@click.command()
@click.option('--gvcf', required=True, type=click.Path(exists=True), help='Path to the .gvcf file')
@click.option('--batch-size', default=1000, help='Number of variants to process in each batch')
@click.option('--force', is_flag=True, help='Force load even if sample already exists in the database')
@click.pass_context
def load(ctx, gvcf, batch_size, force):
    """Load data from a .gvcf file into the database."""
    config = ctx.obj['CONFIG']
    db_config = config['database']
    
    # Get absolute path for tracking
    gvcf_path = os.path.abspath(gvcf)
    
    # Connect to database
    db_adapter = DatabaseAdapter(
        db_type=db_config['type'],
        db_name=db_config['name'],
        host=db_config.get('host'),
        port=db_config.get('port'),
        user=db_config.get('user'),
        password=db_config.get('password')
    )
    
    # Parse sample name from the GVCF
    gvcf_parser = GVCFParser(gvcf_path)
    sample_name = gvcf_parser.get_sample_name()
    
    # Check if sample already exists
    session = db_adapter.get_session()
    existing_sample = session.query(Sample).filter(Sample.name == sample_name).first()
    
    if existing_sample and not force:
        logging.warning(f"Sample '{sample_name}' already exists in the database. Use --force to reload.")
        session.close()
        db_adapter.close()
        raise click.Abort()
    
    logging.info(f"Loading data for sample '{sample_name}' from {gvcf_path} into the database with batch size {batch_size}...")
    
    # Load the variants
    updater = Updater(db_adapter, gvcf_parser, batch_size)
    (tot_inserted, tot_updated) = updater.insert_sample()
    logging.info(f"Inserted a total of '{tot_inserted}' and updated '{tot_updated}' variants.")
    
    session.close()
    db_adapter.close()
    logging.info(f"Data loading for sample '{sample_name}' complete.")
