import click
import logging
import gzip
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
    
    # Determine if output should be compressed
    is_gzipped = output.endswith('.vcf.gz')
    
    # Write to VCF file
    open_func = gzip.open if is_gzipped else open
    mode = 'wt' if is_gzipped else 'w'
    
    with open_func(output, mode) as file:
        # Write header
        file.write("##fileformat=VCFv4.2\n")
        file.write('##INFO=<ID=MEAN_AF,Number=1,Type=Float,Description="Mean non-reference allele frequency">\n')
        file.write('##INFO=<ID=SD_AF,Number=1,Type=Float,Description="Standard deviation of non-reference allele frequency">\n')
        file.write('##INFO=<ID=MAX_AF,Number=1,Type=Float,Description="Maximum non-reference allele frequency">\n')
        file.write('##INFO=<ID=MIN_AF,Number=1,Type=Float,Description="Minimum non-reference allele frequency">\n')
        file.write('##INFO=<ID=DEPTH,Number=1,Type=Float,Description="Total sequencing depth">\n')
        file.write('##INFO=<ID=SAMPLES,Number=1,Type=Integer,Description="Number of samples">\n')
        file.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        
        # Write variants
        for variant in variants:
            mean_af = variant.mean_non_ref_af if variant.mean_non_ref_af is not None else 0.0
            sd_af = variant.sd_non_ref_af if variant.sd_non_ref_af is not None else 0.0
            max_af = variant.max_non_ref_af if variant.max_non_ref_af is not None else 0.0
            min_af = variant.min_non_ref_af if variant.min_non_ref_af is not None else 0.0
            total_depth = variant.total_depth if variant.total_depth is not None else 0.0
            number_of_samples = variant.number_of_samples if variant.number_of_samples is not None else 0
            
            info = (
                f"MEAN_AF={mean_af:.6f};"
                f"MAX_AF={max_af:.6f};"
                f"SD_AF={sd_af:.6f};"
                f"MIN_AF={min_af:.6f};"
                f"DEPTH={total_depth:.1f};"
                f"SAMPLES={number_of_samples}"
            )
            
            file.write(f"{variant.chr}\t{variant.pos}\t.\tN\t<NON_REF>\t.\tPASS\t{info}\n")
    
    session.close()
    db_adapter.close()
    
    logging.info(f"Export complete. Wrote {len(variants)} variants to {output}")
    click.echo(f"Exported {len(variants)} variants to {output}")
