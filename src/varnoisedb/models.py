from sqlalchemy import Column, Integer, String, Float, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Variant(Base):
    __tablename__ = 'variants'
    
    chr = Column(String, primary_key=True)
    pos = Column(Integer, primary_key=True)
    mean_non_ref_af = Column(Float)
    max_non_ref_af = Column(Float)
    min_non_ref_af = Column(Float)
    total_depth = Column(Float)
    number_of_samples = Column(Integer)
    samples = Column(Text)  # JSON serialized
