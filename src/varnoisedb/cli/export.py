import click
import sqlite3

@click.command()
def export():
    """Export the database in VCF format."""
    click.echo("Exporting database to VCF format...")
    
    conn = sqlite3.connect('variants.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM variants')
    variants = cursor.fetchall()
    
    with open('exported_variants.vcf', 'w') as file:
        file.write("##fileformat=VCFv4.2\n")
        file.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n")
        
        for variant in variants:
            chr, pos, mean_non_ref_af, max_non_ref_af, min_non_ref_af = variant
            info = f"MEAN_NON_REF_AF={mean_non_ref_af};MAX_NON_REF_AF={max_non_ref_af};MIN_NON_REF_AF={min_non_ref_af}"
            file.write(f"{chr}\t{pos}\t.\tN\t<NON_REF>\t.\t.\t{info}\n")
    
    conn.close()
    click.echo("Export complete.")
