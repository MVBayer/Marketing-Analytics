from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, MetaData, Table, Boolean

metadata = MetaData()

# Customer touchpoints table
touchpoints = Table(
    'touchpoints', metadata,
    Column('id', Integer, primary_key=True),
    Column('customer_id', Integer, index=True),
    Column('touchpoint', String),
    Column('timestamp', DateTime, index=True),
    Column('channel_cost', Float),
    Column('channel_type', String),
    Column('is_conversion', Boolean),  
    Column('purchase_value', Float)
)

# Aggregated results table
attribution_results = Table(
    'attribution_results', metadata,
    Column('model_type', String),
    Column('channel', String),
    Column('attribution_percentage', Float),
    Column('attributed_conversions', Integer),
    Column('total_appearances', Integer),
    Column('success_rate', Float),
    Column('total_cost', Float),
    Column('total_revenue', Float),
    Column('roi', Float)
)

# Explicitly export metadata
__all__ = ['metadata', 'touchpoints', 'attribution_results']