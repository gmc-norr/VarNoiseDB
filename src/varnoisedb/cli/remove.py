import click
import logging
import os
from varnoisedb.database import DatabaseAdapter
from varnoisedb.updater import Updater
from varnoisedb.gvcf_parser import GVCFParser
from varnoisedb.models import Sample

logging.basicConfig(level=logging.INFO)

@click.command()
@click.argument('sample_name')
@click.option('--gvcf', type=click.Path(exists=True), help='Path to the .gvcf file')
@click.pass_context
def remove(ctx, sample_name, gvcf):
    """Remove a sample and its variants from the database."""
    config = ctx.obj['CONFIG']
    db_config = config['database']
    
    # Connect to database
    db_adapter = DatabaseAdapter(
        db_type=db_config['type'],
        db_name=db_config['name'],
        host=db_config.get('host'),
        port=db_config.get('port'),
        user=db_config.get('user'),
        password=db_config.get('password')
    )
    
    session = db_adapter.get_session()
    
    # Check if sample exists
    existing_sample = session.query(Sample).filter(Sample.name == sample_name).first()
    
    if not existing_sample:
        logging.warning(f"Sample '{sample_name}' does not exist in the database.")
        session.close()
        db_adapter.close()
        raise click.Abort()
    
    # Use provided GVCF or find the GVCF path from the samples table
    gvcf_path = gvcf if gvcf else existing_sample.gvcf_path
    
    if not os.path.exists(gvcf_path):
        logging.warning(f"GVCF file '{gvcf_path}' does not exist.")
        session.close()
        db_adapter.close()
        raise click.Abort()
    
    logging.info(f"Removing data for sample '{sample_name}' using GVCF file '{gvcf_path}'...")
    
    # Parse the GVCF file
    gvcf_parser = GVCFParser(gvcf_path)
    
    # Remove the sample and its variants
    updater = Updater(db_adapter, gvcf_parser, batch_size=1000)
    updater.remove_sample()
    
    session.close()
    db_adapter.close()
    logging.info(f"Data removal for sample '{sample_name}' complete.")
