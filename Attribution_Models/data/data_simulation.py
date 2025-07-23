import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

class CustomerJourneySimulator:
    def __init__(self):
        # Channel classifications and costs
        self.paid_channels = {
            'Facebook Ad': {'min_cost': 2.0, 'max_cost': 5.0},
            'Google Search': {'min_cost': 1.5, 'max_cost': 4.0},
            'Instagram Ad': {'min_cost': 2.5, 'max_cost': 6.0},
            'YouTube Ad': {'min_cost': 3.0, 'max_cost': 7.0}
        }
        
        self.organic_channels = [
            'Website Visit',
            'Email Newsletter',
            'Discount Code Email',
            'Recommend a Friend',
            'Blog Post View',
            'Product Review'
        ]
        
        # Combined touchpoints for easy access
        self.touchpoints = list(self.paid_channels.keys()) + self.organic_channels
        self.first_touch_points = list(self.paid_channels.keys())  # Only paid channels as first touch
        
        # Purchase value parameters
        self.purchase_value_params = {
            'min_value': 50,
            'max_value': 500,
            'mean_value': 150,
            'std_value': 75
        }
    
    def generate_purchase_value(self):
        """Generate a random purchase value following a truncated normal distribution."""
        value = np.random.normal(
            self.purchase_value_params['mean_value'],
            self.purchase_value_params['std_value']
        )
        # Ensure value is within bounds
        return max(
            self.purchase_value_params['min_value'],
            min(value, self.purchase_value_params['max_value'])
        )
    
    def get_channel_cost(self, channel):
        """Get cost for a paid channel, return 0 for organic channels."""
        if channel in self.paid_channels:
            channel_costs = self.paid_channels[channel]
            return round(np.random.uniform(
                channel_costs['min_cost'],
                channel_costs['max_cost']
            ), 2)
        return 0.0

    def generate_customer_journey(self, customer_id, start_date):
        """Generate a single customer journey with possible multiple purchases."""
        journey_length = np.random.randint(3, 8)
        will_purchase = np.random.choice([True, False], p=[0.3, 0.7])
        
        journey = []
        current_date = start_date
        
        for i in range(journey_length):
            current_date += timedelta(hours=np.random.randint(1, 48))
            
            touchpoint = (np.random.choice(self.first_touch_points) if i == 0 
                        else np.random.choice(self.touchpoints))
            
            channel_cost = self.get_channel_cost(touchpoint)
            
            journey_point = {
                'customer_id': customer_id,
                'touchpoint': touchpoint,
                'timestamp': current_date,
                'channel_cost': channel_cost,
                'channel_type': 'paid' if touchpoint in self.paid_channels else 'organic',
                'is_conversion': False,  
                'purchase_value': 0  
            }
            
            journey.append(journey_point)
            
            if will_purchase and i == journey_length - 1:
                current_date += timedelta(hours=np.random.randint(1, 24))
                purchase_value = self.generate_purchase_value()
                journey.append({
                    'customer_id': customer_id,
                    'touchpoint': 'Purchase',
                    'timestamp': current_date,
                    'channel_cost': 0,
                    'channel_type': None,  
                    'is_conversion': True,  
                    'purchase_value': purchase_value
                })
                
                # 20% chance of repeat purchase journey
                if np.random.random() < 0.2:
                    repeat_journey = self.generate_customer_journey(
                        customer_id,
                        current_date + timedelta(days=np.random.randint(15, 45))
                    )
                    journey.extend(repeat_journey)
        
        return journey

    def create_dataset(self, num_customers=200, start_date='2023-01-01'):
        """Create a dataset with multiple customer journeys."""
        all_journeys = []
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        
        for customer_id in range(1, num_customers + 1):
            customer_start = start_datetime + timedelta(days=np.random.randint(0, 30))
            journey = self.generate_customer_journey(customer_id, customer_start)
            all_journeys.extend(journey)
        
        df = pd.DataFrame(all_journeys)
        return df.sort_values('timestamp')