"""
This module updates the database with parsed gvcf data.
It calculates and updates the mean, max, and min allele frequencies.
"""

import json
from sqlalchemy.orm import sessionmaker
from varnoisedb.models import Variant

class Updater:
    def __init__(self, db_adapter, gvcf_parser, batch_size=1000):
        self.db_adapter = db_adapter
        self.gvcf_parser = gvcf_parser
        self.batch_size = batch_size
    
    def update_database(self):
        session = self.db_adapter.get_session()
        batch = []
        for chr, pos, non_ref_af, dp, sample_name in self.gvcf_parser.parse():
            batch.append((chr, pos, non_ref_af, dp, sample_name))
            if len(batch) >= self.batch_size:
                self._execute_batch(session, batch)
                batch.clear()
        
        if batch:
            self._execute_batch(session, batch)
        
        session.commit()
        session.close()
    
    def _execute_batch(self, session, batch):
        positions = [(chr, pos) for chr, pos, _, _, _ in batch]
        existing_records = session.query(Variant).filter(
            (Variant.chr, Variant.pos).in_(positions)
        ).all()
        existing_dict = {(record.chr, record.pos): record for record in existing_records}
        
        for record in batch:
            chr, pos, non_ref_af, dp, sample_name = record
            if (chr, pos) in existing_dict:
                variant = existing_dict[(chr, pos)]
                variant.number_of_samples += 1
                variant.total_depth += dp
                variant.mean_non_ref_af = ((variant.mean_non_ref_af * (variant.number_of_samples - 1)) + non_ref_af) / variant.number_of_samples
                variant.max_non_ref_af = max(variant.max_non_ref_af, non_ref_af)
                variant.min_non_ref_af = min(variant.min_non_ref_af, non_ref_af)
                samples = json.loads(variant.samples)
                if sample_name not in samples:
                    samples.append(sample_name)
                variant.samples = json.dumps(samples)
            else:
                variant = Variant(
                    chr=chr,
                    pos=pos,
                    mean_non_ref_af=non_ref_af,
                    max_non_ref_af=non_ref_af,
                    min_non_ref_af=non_ref_af,
                    total_depth=dp,
                    number_of_samples=1,
                    samples=json.dumps([sample_name])
                )
                session.add(variant)
