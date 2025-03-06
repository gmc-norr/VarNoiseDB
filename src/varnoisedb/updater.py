"""
This module updates the database with parsed gvcf data.
It calculates and updates the mean, max, and min allele frequencies.
"""

import json
from sqlalchemy import tuple_
from sqlalchemy.orm import Session

from varnoisedb.models import Variant, Sample
from varnoisedb.utils import update_stats

class Updater:
    def __init__(self, db_adapter, gvcf_parser, batch_size=1000):
        self.db_adapter = db_adapter
        self.gvcf_parser = gvcf_parser
        self.batch_size = batch_size
    
    def insert_sample(self):
        session = self.db_adapter.get_session()

        tot_inserted = 0
        tot_updated = 0
        for batch in self.gvcf_parser.parse_batch(self.batch_size):
            (n_inserted, n_updated) = self._insert_batch(session, batch)
            tot_inserted += n_inserted
            tot_updated += n_updated
        
        self._update_samples(session)
        

        session.commit()
        session.close()
        return (tot_inserted, tot_updated)

    def remove_sample(self):
        session = self.db_adapter.get_session()
        sample_name = self.gvcf_parser.get_sample_name()
        sample = session.query(Sample).filter(Sample.name == sample_name).first()
        if sample:
            for batch in self.gvcf_parser.parse_batch(self.batch_size):
                self._remove_batch(session, batch)
            
            session.delete(sample)
            session.commit()
            session.close()
        else:
            session.close()
            raise ValueError(f"Sample '{sample_name}' not found in the database.")
    
    def _insert_batch(self, session: Session, batch: list[tuple]):
        """Execute batch operations efficiently using SQLAlchemy bulk operations."""
        # Extract unique positions for querying
        positions = [(chr, pos) for chr, pos, _, _, _ in batch]
        
        # Fetch all existing records in one query
        existing_records = session.query(Variant).filter(
            tuple_(Variant.chr, Variant.pos).in_(positions)
        ).all()
        
        existing_dict = {(record.chr, record.pos): record for record in existing_records}
        
        # Prepare bulk inserts and updates
        to_insert = []
        to_update = []
        
        # Process all records in the batch
        for chr, pos, non_ref_af, dp, sample_name in batch:
            if (chr, pos) in existing_dict:
                # Update existing record
                variant = existing_dict[(chr, pos)]
                
                # Calculate new values
                new_mean, new_sd, new_number = update_stats(
                    variant.mean_non_ref_af, 
                    variant.sd_non_ref_af, 
                    variant.number_of_samples,
                    non_ref_af,
                    operation='add'
                )
                new_min = variant.min_non_ref_af
                new_max = variant.max_non_ref_af
                new_max_af_sample = variant.max_non_ref_af_sample
                new_min_af_sample = variant.min_non_ref_af_sample
                if non_ref_af > variant.max_non_ref_af:
                    new_max = non_ref_af
                    new_max_af_sample = sample_name
                if non_ref_af < variant.min_non_ref_af:
                    new_min = non_ref_af
                    new_min_af_sample = sample_name
        
                new_depth = float(variant.total_depth) + float(dp)
                
                # Add to update list
                to_update.append({
                    'chr': chr,
                    'pos': pos,
                    'mean_non_ref_af': new_mean,
                    'sd_non_ref_af': new_sd,
                    'max_non_ref_af': new_max,
                    'min_non_ref_af': new_min,
                    'total_depth': new_depth,
                    'number_of_samples': new_number,
                    'max_non_ref_af_sample': new_min_af_sample,
                    'min_non_ref_af_sample': new_max_af_sample,
                })
            else:
                # Insert new record
                to_insert.append({
                    'chr': chr,
                    'pos': pos,
                    'mean_non_ref_af': float(non_ref_af),
                    'sd_non_ref_af': float(non_ref_af),
                    'max_non_ref_af': float(non_ref_af),
                    'min_non_ref_af': float(non_ref_af),
                    'total_depth': float(dp),
                    'number_of_samples': 1,
                    'max_non_ref_af_sample': sample_name,
                    'min_non_ref_af_sample': sample_name
                })
        
        # Perform bulk operations
        if to_insert:
            session.bulk_insert_mappings(Variant, to_insert)
        
        if to_update:
            session.bulk_update_mappings(Variant, to_update)

        return(len(to_insert), len(to_update))

    def _remove_batch(self, session: Session, batch: list[tuple]):
        """Execute batch operations efficiently using SQLAlchemy bulk operations."""
        # Extract unique positions for querying
        positions = [(chr, pos) for chr, pos, _, _, _ in batch]
        
        # Fetch all existing records in one query
        existing_records = session.query(Variant).filter(
            tuple_(Variant.chr, Variant.pos).in_(positions)
        ).all()
        
        existing_dict = {(record.chr, record.pos): record for record in existing_records}
        
        # Prepare bulk updates and deletions
        to_update = []
        to_delete = []
        
        # Process all records in the batch
        for chr, pos, non_ref_af, dp, sample_name in batch:
            if (chr, pos) in existing_dict:
                # Update existing record
                variant = existing_dict[(chr, pos)]

                # Calculate new values
                new_mean, new_sd, new_number = update_stats(
                    variant.mean_non_ref_af, 
                    variant.sd_non_ref_af, 
                    variant.number_of_samples,
                    non_ref_af,
                    operation='remove'
                )
                new_min = variant.min_non_ref_af
                new_max = variant.max_non_ref_af
                # if the removed sample happened to be the max or min, we lose the min max info
                if variant.max_non_ref_af_sample == sample_name:
                    new_max = None
                    new_max_af_sample = None
                if variant.min_non_ref_af_sample == sample_name:
                    new_min = None
                    new_min_af_sample = None
        
                new_depth = float(variant.total_depth) - float(dp)
                
                if new_number == 0:
                    # Add to delete list
                    to_delete.append(variant)
                else:
                    # Add to update list
                    to_update.append({
                        'chr': chr,
                        'pos': pos,
                        'mean_non_ref_af': new_mean,
                        'sd_non_ref_af': new_sd,
                        'max_non_ref_af': new_max,
                        'min_non_ref_af': new_min,
                        'total_depth': new_depth,
                        'number_of_samples': new_number,
                        'max_non_ref_af_sample': new_min_af_sample,
                        'min_non_ref_af_sample': new_max_af_sample,
                    })
        
        # Perform bulk operations
        if to_update:
            session.bulk_update_mappings(Variant, to_update)
        
        if to_delete:
            for variant in to_delete:
                session.delete(variant)
    
    def _update_samples(self, session: Session):
        """Update the samples table with the sample information."""
        sample_name = self.gvcf_parser.get_sample_name()
        gvcf_path = self.gvcf_parser.gvcf_file
        
        # Check if the sample already exists
        existing_sample = session.query(Sample).filter(Sample.name == sample_name).first()
        
        if existing_sample:
            # Update the existing sample record
            existing_sample.gvcf_path = gvcf_path
        else:
            # Insert a new sample record
            new_sample = Sample(name=sample_name, gvcf_path=gvcf_path)
            session.add(new_sample)
