from cyvcf2 import VCF

class GVCFParser:
    def __init__(self, gvcf_file):
        self.gvcf_file = gvcf_file
    
    def parse(self):
        vcf = VCF(self.gvcf_file)
        sample_name = vcf.samples[0]
        for record in vcf:
            chr = record.CHROM
            pos = record.POS
            ad = record.format('AD')[0]
            dp = record.format('DP')[0]
            alt_alleles = record.ALT
            non_ref_index = alt_alleles.index('<NON_REF>') + 1 if '<NON_REF>' in alt_alleles else None
            if non_ref_index:
                non_ref_af = ad[non_ref_index] / dp
                yield chr, pos, non_ref_af, dp, sample_name
