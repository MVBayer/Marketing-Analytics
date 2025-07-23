from dotenv import load_dotenv
load_dotenv()

from .config import config 
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from .models.attribution_models import SingleTouchAttribution, MultiTouchAttribution 
from .database.connection import DatabaseConnection  

# Marketing channel classifications
PAID_CHANNELS = [
    'Facebook Ad',
    'Google Search',
    'Instagram Ad',
    'YouTube Ad'
]

ORGANIC_CHANNELS = [
    'Website Visit',
    'Email Newsletter',
    'Discount Code Email',
    'Recommend a Friend',
    'Blog Post View',
    'Product Review'
]


def load_data_to_db(csv_path, db_connection):
    """Load CSV data into SQLite database."""
    print(f"Loading data from {csv_path}...")
    df = pd.read_csv(csv_path)
    
    # Convert data types to match schema
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['is_conversion'] = df['is_conversion'].astype(bool)
    df['channel_cost'] = df['channel_cost'].astype(float)
    df['purchase_value'] = df['purchase_value'].astype(float)
    
        
    print(f"Total records to process: {len(df):,}")
    
    # Load data in chunks of 1000 rows
    chunk_size = 1000
    
    with db_connection.get_session() as session:
        # Drop existing data to avoid duplicates
        session.execute(text("DELETE FROM touchpoints"))
        session.commit()
        print("Existing data cleared. Loading new data in chunks...")
        
        # Load data in chunks
        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i+chunk_size]
            try:
                chunk.to_sql('touchpoints', session.bind, 
                           if_exists='append', 
                           index=False,
                           method='multi')
                session.commit()
                print(f"Processed chunk {i//chunk_size + 1} of {(len(df)-1)//chunk_size + 1}")
            except Exception as e:
                print(f"Error loading chunk {i//chunk_size + 1}: {str(e)}")
                session.rollback()
                raise
    
    print("Data loading completed successfully.")
        

def analyze_channel_performance(df, channel_type, model_name=None):
    """Print analysis for specified channel type."""
    if model_name:
        print(f"\n{model_name} - {channel_type} Channels Performance:")
    else:
        print(f"\n{channel_type} Channels Performance:")
    print("--------------------------------")
    # Update metrics to match the SQL query output
    metrics = [
        'attribution_percentage',
        'attributed_conversions',
        'total_appearances',
        'success_rate',
        'total_cost',
        'roi'
    ]
    
    # Create a copy of the DataFrame with formatted columns
    formatted_df = df[metrics].copy()
    
    # Format percentage columns
    percentage_cols = ['attribution_percentage', 'success_rate', 'roi']
    for col in percentage_cols:
        if col in formatted_df.columns:
            formatted_df[col] = formatted_df[col].map('{:.2%}'.format)
    
    # Format integer columns
    integer_cols = ['attributed_conversions', 'total_appearances']
    for col in integer_cols:
        if col in formatted_df.columns:
            formatted_df[col] = formatted_df[col].map('{:,.0f}'.format)
    
    # Format currency columns
    if 'total_cost' in formatted_df.columns:
        formatted_df['total_cost'] = formatted_df['total_cost'].map('${:,.2f}'.format)
    
    print(formatted_df)
    
def main():
    # Ensure directories exist
    config.ensure_directories()
    
    # Initialize database
    db = DatabaseConnection()
    db.init_db()
    
    # Load data if needed
    load_data_to_db('data/raw/customer_touchpoints_simulated3.csv', db)
    
    # Initialize attribution models
    single_touch_model = SingleTouchAttribution(db)
    multi_touch_model = MultiTouchAttribution(db)
    
    # Calculate metrics for first and last touch
    first_touch_df = single_touch_model.calculate_channel_metrics('first')
    last_touch_df = single_touch_model.calculate_channel_metrics('last')
    
    # Analyze and display results
    print("\n=== First Touch Attribution Analysis ===")
    analyze_channel_performance(first_touch_df[first_touch_df.index.isin(PAID_CHANNELS)], "Paid")
    analyze_channel_performance(first_touch_df[first_touch_df.index.isin(ORGANIC_CHANNELS)], "Organic")
    
    print("\n=== Last Touch Attribution Analysis ===")
    analyze_channel_performance(last_touch_df[last_touch_df.index.isin(PAID_CHANNELS)], "Paid")
    analyze_channel_performance(last_touch_df[last_touch_df.index.isin(ORGANIC_CHANNELS)], "Organic")
    
# Multi-touch attribution analysis
    print("\n=== Multi-Touch Attribution Analysis ===")
    
    try:
        # Calculate U-shaped attribution
        print("Calculating U-Shaped attribution...")
        u_shaped_df = multi_touch_model.calculate_channel_metrics('u_shaped')
        
        # Verify that attribution percentages sum to approximately 1.0
        total_attribution = u_shaped_df['attribution_percentage'].sum()
        print(f"Total U-shaped attribution percentage: {total_attribution:.2f}")
        
        # Calculate W-shaped attribution
        print("\nCalculating W-Shaped attribution...")
        w_shaped_df = multi_touch_model.calculate_channel_metrics('w_shaped')
        
        # Verify that attribution percentages sum to approximately 1.0
        total_attribution = w_shaped_df['attribution_percentage'].sum()
        print(f"Total W-shaped attribution percentage: {total_attribution:.2f}")
        
        # Calculate Linear attribution
        print("\nCalculating Linear attribution...")
        linear_df = multi_touch_model.calculate_channel_metrics('linear')
        
        # Verify that attribution percentages sum to approximately 1.0
        total_attribution = linear_df['attribution_percentage'].sum()
        print(f"Total Linear attribution percentage: {total_attribution:.2f}")
        
        # Calculate Time Decay attribution
        print("\nCalculating Time-Decay attribution...")
        time_decay_df = multi_touch_model.calculate_channel_metrics('time_decay')
        
        # Verify that attribution percentages sum to approximately 1.0
        total_attribution = time_decay_df['attribution_percentage'].sum()
        print(f"Total Time-Decay attribution percentage: {total_attribution:.2f}")
        
        # Display results for each model
        print("\n=== U-Shaped Attribution Analysis ===")
        analyze_channel_performance(u_shaped_df[u_shaped_df.index.isin(PAID_CHANNELS)], "Paid", "U-Shaped")
        analyze_channel_performance(u_shaped_df[u_shaped_df.index.isin(ORGANIC_CHANNELS)], "Organic", "U-Shaped")
        
        print("\n=== W-Shaped Attribution Analysis ===")
        analyze_channel_performance(w_shaped_df[w_shaped_df.index.isin(PAID_CHANNELS)], "Paid", "W-Shaped")
        analyze_channel_performance(w_shaped_df[w_shaped_df.index.isin(ORGANIC_CHANNELS)], "Organic", "W-Shaped")
        
        print("\n=== Linear Attribution Analysis ===")
        analyze_channel_performance(linear_df[linear_df.index.isin(PAID_CHANNELS)], "Paid", "Linear")
        analyze_channel_performance(linear_df[linear_df.index.isin(ORGANIC_CHANNELS)], "Organic", "Linear")
        
        print("\n=== Time-Decay Attribution Analysis ===")
        analyze_channel_performance(time_decay_df[time_decay_df.index.isin(PAID_CHANNELS)], "Paid", "Time-Decay")
        analyze_channel_performance(time_decay_df[time_decay_df.index.isin(ORGANIC_CHANNELS)], "Organic", "Time-Decay")
        
        # Save detailed results to Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        results_path = config.get_attribution_results_path('attribution_comparison', timestamp).with_suffix('.xlsx')
        
        # Save results to Excel with all models
        with pd.ExcelWriter(results_path) as writer:
            first_touch_df.to_excel(writer, sheet_name='First Touch')
            last_touch_df.to_excel(writer, sheet_name='Last Touch')
            u_shaped_df.to_excel(writer, sheet_name='U-Shaped')
            w_shaped_df.to_excel(writer, sheet_name='W-Shaped')
            linear_df.to_excel(writer, sheet_name='Linear')
            time_decay_df.to_excel(writer, sheet_name='Time-Decay')
            
            # Build summary comparison for all models
            summary = pd.DataFrame({
                'First Touch Attribution %': first_touch_df['attribution_percentage'],
                'First Touch ROI': first_touch_df['roi'],
                'Last Touch Attribution %': last_touch_df['attribution_percentage'],
                'Last Touch ROI': last_touch_df['roi'],
                'U-Shaped Attribution %': u_shaped_df['attribution_percentage'],
                'U-Shaped ROI': u_shaped_df['roi'],
                'W-Shaped Attribution %': w_shaped_df['attribution_percentage'],
                'W-Shaped ROI': w_shaped_df['roi'],
                'Linear Attribution %': linear_df['attribution_percentage'],
                'Linear ROI': linear_df['roi'],
                'Time-Decay Attribution %': time_decay_df['attribution_percentage'],
                'Time-Decay ROI': time_decay_df['roi']
            })
            
            summary['Total Cost'] = first_touch_df['total_cost']
            summary.to_excel(writer, sheet_name='Summary')
        
        print(f"\nDetailed results saved to: {results_path}")
    
    except Exception as e:
        print(f"Error calculating attribution models: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()