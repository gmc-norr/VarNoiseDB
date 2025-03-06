import click
import logging
from varnoisedb.database import DatabaseAdapter
from varnoisedb.models import Variant
import os

@click.command()
@click.option('--output', '-o', default='variants.vcf', help='Output VCF file path')
@click.pass_context
def export(ctx, output):
    """Export the database in VCF format with variant statistics in the INFO field."""
    config = ctx.obj['CONFIG']
    db_config = config['database']
    
    logging.info(f"Exporting database to VCF format in {output}...")
    
    # Connect to the database
    db_adapter = DatabaseAdapter(
        db_type=db_config['type'],
        db_name=db_config['name'],
        host=db_config.get('host'),
        port=db_config.get('port'),
        user=db_config.get('user'),
        password=db_config.get('password')
    )
    
    session = db_adapter.get_session()
    
    # Query all variants
    variants = session.query(Variant).order_by(Variant.chr, Variant.pos).all()
    
    # Write to VCF file
    with open(output, 'w') as file:
        # Write header
        file.write("##fileformat=VCFv4.2\n")
        file.write('##INFO=<ID=MEAN_AF,Number=1,Type=Float,Description="Mean non-reference allele frequency">\n')
        file.write('##INFO=<ID=MAX_AF,Number=1,Type=Float,Description="Maximum non-reference allele frequency">\n')
        file.write('##INFO=<ID=MIN_AF,Number=1,Type=Float,Description="Minimum non-reference allele frequency">\n')
        file.write('##INFO=<ID=DEPTH,Number=1,Type=Float,Description="Total sequencing depth">\n')
        file.write('##INFO=<ID=SAMPLES,Number=1,Type=Integer,Description="Number of samples">\n')
        file.write('##INFO=<ID=SAMPLE_NAMES,Number=.,Type=String,Description="Names of samples">\n')
        file.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        
        # Write variants
        for variant in variants:
            info = (
                f"MEAN_AF={variant.mean_non_ref_af:.6f};"
                f"MAX_AF={variant.max_non_ref_af:.6f};"
                f"MIN_AF={variant.min_non_ref_af:.6f};"
                f"DEPTH={variant.total_depth:.1f};"
                f"SAMPLES={variant.number_of_samples};"
                f"SAMPLE_NAMES={variant.samples}"
            )
            
            file.write(f"{variant.chr}\t{variant.pos}\t.\tN\t<NON_REF>\t.\tPASS\t{info}\n")
    
    session.close()
    db_adapter.close()
    
    logging.info(f"Export complete. Wrote {len(variants)} variants to {output}")
    click.echo(f"Exported {len(variants)} variants to {output}")
