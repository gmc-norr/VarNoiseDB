from sqlalchemy import Column, Integer, String, Float, Text, MetaData, DateTime
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone

# This ensures consistent naming conventions
metadata = MetaData()
Base = declarative_base(metadata=metadata)

class Variant(Base):
    __tablename__ = 'variants'
    
    chr = Column(String(50), primary_key=True)
    pos = Column(Integer, primary_key=True)
    mean_non_ref_af = Column(Float)
    sd_non_ref_af = Column(Float)
    max_non_ref_af = Column(Float)
    min_non_ref_af = Column(Float)
    total_depth = Column(Float)
    number_of_samples = Column(Integer)
    max_non_ref_af_sample = Column(String(255))
    min_non_ref_af_sample = Column(String(255))

class Sample(Base):
    __tablename__ = 'samples'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    gvcf_path = Column(String(1024), nullable=False)
    date_added = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    
    def __repr__(self):
        return f"<Sample(name='{self.name}', gvcf_path='{self.gvcf_path}')>"
