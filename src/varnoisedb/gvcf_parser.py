from cyvcf2 import VCF

class GVCFParser:
    def __init__(self, gvcf_file):
        self.gvcf_file = gvcf_file
        self._sample_name = None
    
    def get_sample_name(self):
        """Get the name of the sample from the GVCF file."""
        if self._sample_name is None:
            vcf = VCF(self.gvcf_file)
            self._sample_name = vcf.samples[0] if vcf.samples else "unknown"
        return self._sample_name
    
    def parse(self):
        vcf = VCF(self.gvcf_file)
        sample_name = vcf.samples[0]
        self._sample_name = sample_name  # Store for future use
        
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
    
    def parse_batch(self, batch_size):
        vcf = VCF(self.gvcf_file)
        sample_name = vcf.samples[0]
        self._sample_name = sample_name  # Store for future use
        
        batch = []
        for record in vcf:
            chr = record.CHROM
            pos = record.POS
            ad = record.format('AD')[0]
            dp = record.format('DP')[0]
            alt_alleles = record.ALT
            non_ref_index = alt_alleles.index('<NON_REF>') + 1 if '<NON_REF>' in alt_alleles else None
            if non_ref_index:
                non_ref_af = ad[non_ref_index] / dp
                batch.append((chr, pos, non_ref_af, dp, sample_name))
                if len(batch) >= batch_size:
                    yield batch
                    batch = []
        
        if batch:
            yield batch
