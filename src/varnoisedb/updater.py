"""
This module updates the database with parsed gvcf data.
It calculates and updates the mean, max, and min allele frequencies.
"""

import json

class Updater:
    def __init__(self, db_adapter, gvcf_parser, batch_size=1000):
        self.db_adapter = db_adapter
        self.gvcf_parser = gvcf_parser
        self.batch_size = batch_size
    
    def update_database(self):
        batch = []
        for chr, pos, non_ref_af, dp, sample_name in self.gvcf_parser.parse():
            batch.append((chr, pos, non_ref_af, dp, sample_name))
            if len(batch) >= self.batch_size:
                self._execute_batch(batch)
                batch.clear()
        
        if batch:
            self._execute_batch(batch)
    
    def _execute_batch(self, batch):
        positions = [(chr, pos) for chr, pos, _, _, _ in batch]
        placeholders = ', '.join(['(?, ?)'] * len(positions))
        query = f'''
        SELECT chr, pos, mean_non_ref_af, max_non_ref_af, min_non_ref_af, total_depth, number_of_samples, samples
        FROM variants WHERE (chr, pos) IN ({placeholders})
        '''
        existing_records = self.db_adapter.execute(query, [item for sublist in positions for item in sublist]).fetchall()
        existing_dict = {(chr, pos): (mean_non_ref_af, max_non_ref_af, min_non_ref_af, total_depth, number_of_samples, samples)
                         for chr, pos, mean_non_ref_af, max_non_ref_af, min_non_ref_af, total_depth, number_of_samples, samples in existing_records}
        
        for record in batch:
            chr, pos, non_ref_af, dp, sample_name = record
            if (chr, pos) in existing_dict:
                old_mean_non_ref_af, old_max_non_ref_af, old_min_non_ref_af, old_total_depth, old_number_of_samples, old_samples = existing_dict[(chr, pos)]
                new_number_of_samples = old_number_of_samples + 1
                new_total_depth = old_total_depth + dp
                new_mean_non_ref_af = ((old_mean_non_ref_af * old_number_of_samples) + non_ref_af) / new_number_of_samples
                new_max_non_ref_af = max(old_max_non_ref_af, non_ref_af)
                new_min_non_ref_af = min(old_min_non_ref_af, non_ref_af)
                samples = json.loads(old_samples)
                if sample_name not in samples:
                    samples.append(sample_name)
                new_samples = json.dumps(samples)
            else:
                new_number_of_samples = 1
                new_total_depth = dp
                new_mean_non_ref_af = non_ref_af
                new_max_non_ref_af = non_ref_af
                new_min_non_ref_af = non_ref_af
                new_samples = json.dumps([sample_name])
            
            self.db_adapter.execute('''
            INSERT OR REPLACE INTO variants (chr, pos, mean_non_ref_af, max_non_ref_af, min_non_ref_af, total_depth, number_of_samples, samples)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (chr, pos, new_mean_non_ref_af, new_max_non_ref_af, new_min_non_ref_af, new_total_depth, new_number_of_samples, new_samples))
