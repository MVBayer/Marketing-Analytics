import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from src.config import config
from data_simulation import CustomerJourneySimulator

# Define filename
raw_data_filename = 'customer_touchpoints_simulated3.csv'

# Ensure directories exist using config
config.ensure_directories()

# Create simulator and generate data
simulator = CustomerJourneySimulator()
df = simulator.create_dataset(num_customers=200)

# Save to CSV using config path with specified filename
output_path = config.get_raw_data_path(raw_data_filename)
df.to_csv(output_path, index=False)

# Print summary statistics
print(f"\nDataset Generation Summary:")
print(f"-------------------------")
print(f"Total rows: {len(df)}")
print(f"Unique customers: {df['customer_id'].nunique()}")
print(f"Total purchases: {len(df[df['touchpoint'] == 'Purchase'])}")
print(f"Data saved to: {output_path}")
