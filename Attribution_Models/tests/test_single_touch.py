import pytest
import pandas as pd
import numpy as np
from src.models.attribution_models import SingleTouchAttribution
import psutil  
from src.database.connection import DatabaseConnection



# =============================================
# SQL Database Tests
# =============================================

@pytest.fixture
def test_db():
    """Create test database connection with in-memory SQLite."""
    return DatabaseConnection("sqlite:///:memory:")

@pytest.fixture
def success_rate_test_data():
    """Create a controlled dataset for testing success rate calculations."""
    return pd.DataFrame({
        'customer_id':    [1, 1, 2, 2, 3, 3, 4, 4, 5, 5],
        'touchpoint':     ['Facebook Ad', 'Purchase',
                        'Facebook Ad', 'Purchase',
                        'Facebook Ad', 'Email',      # No purchase
                        'Instagram Ad', 'Purchase',
                        'Instagram Ad', 'Email'],    # No purchase
        'timestamp':      pd.date_range('2023-01-01', periods=10),
        'channel_cost':   [5.0, 0.0, 5.0, 0.0, 5.0, 0.0, 4.0, 0.0, 4.0, 0.0],
        'channel_type':   ['paid', None,      # Purchase events don't have channel types
                        'paid', None,
                        'paid', 'organic',
                        'paid', None,
                        'paid', 'organic'],
        'is_conversion': [False, True,     # New column to track conversion events
                        False, True,
                        False, False,
                        False, True,
                        False, False],
        'purchase_value': [0.0, 100.0, 0.0, 150.0, 0.0, 0.0, 0.0, 200.0, 0.0, 0.0]
    })

@pytest.fixture
def populated_success_rate_db(test_db, success_rate_test_data):
    """Create test database with success rate test data."""
    with test_db.get_session() as session:
        success_rate_test_data.to_sql('touchpoints', session.bind, index=False)
    return test_db

def test_facebook_success_rate(populated_success_rate_db):
    """Test Facebook Ad success rate calculation.
    
    Success Rate = (Converting customers with FB first touch) / (Total customers who saw FB)
    In test data:
    - 3 customers saw Facebook Ad (customers 1, 2, 3)
    - 2 of them converted (customers 1, 2)
    Expected success rate = 2/3 ≈ 0.667
    """
    model = SingleTouchAttribution(populated_success_rate_db)
    results = model.calculate_channel_metrics('first')
    
    # Test Facebook Ad metrics
    assert results.loc['Facebook Ad', 'attributed_conversions'] == 2
    assert results.loc['Facebook Ad', 'total_appearances'] == 3
    assert np.isclose(results.loc['Facebook Ad', 'success_rate'], 2/3, rtol=1e-3)

def test_instagram_success_rate(populated_success_rate_db):
    """Test Instagram Ad success rate calculation.
    
    Success Rate = (Converting customers with IG first touch) / (Total customers who saw IG)
    In test data:
    - 2 customers saw Instagram Ad (customers 4, 5)
    - 1 of them converted (customer 4)
    Expected success rate = 1/2 = 0.5
    """
    model = SingleTouchAttribution(populated_success_rate_db)
    results = model.calculate_channel_metrics('first')
    
    assert results.loc['Instagram Ad', 'attributed_conversions'] == 1
    assert results.loc['Instagram Ad', 'total_appearances'] == 2
    assert np.isclose(results.loc['Instagram Ad', 'success_rate'], 0.5, rtol=1e-3)

def test_email_success_rate(populated_success_rate_db):
    """Test Email success rate calculation.
    
    Email appears only as non-first touch, so should have 0 attributed conversions
    but still be counted in total appearances
    """
    model = SingleTouchAttribution(populated_success_rate_db)
    results = model.calculate_channel_metrics('first')
    
    assert results.loc['Email', 'attributed_conversions'] == 0
    assert results.loc['Email', 'total_appearances'] == 2
    assert np.isclose(results.loc['Email', 'success_rate'], 0.0, rtol=1e-3)

def test_success_rate_consistency(populated_success_rate_db):
    """Test that success rates are consistent between first and last touch."""
    model = SingleTouchAttribution(populated_success_rate_db)
    first_touch = model.calculate_channel_metrics('first')
    last_touch = model.calculate_channel_metrics('last')
    
    # Total number of converting customers should be same for both methods
    assert first_touch['attributed_conversions'].sum() == last_touch['attributed_conversions'].sum()
    
    # Each channel's total appearances should be same regardless of attribution method
    for channel in first_touch.index:
        assert first_touch.loc[channel, 'total_appearances'] == last_touch.loc[channel, 'total_appearances']
    
    # Success rates should always be between 0 and 1
    assert (first_touch['success_rate'] >= 0).all() and (first_touch['success_rate'] <= 1).all()
    assert (last_touch['success_rate'] >= 0).all() and (last_touch['success_rate'] <= 1).all()

def test_success_rate_edge_cases(populated_success_rate_db):
    """Test success rate calculations for edge cases."""
    model = SingleTouchAttribution(populated_success_rate_db)
    results = model.calculate_channel_metrics('first')
    
    # Verify total success rate makes sense
    total_conversions = 3  # Customers 1, 2, and 4 converted
    total_customers = 5    # Total unique customers
    overall_success_rate = total_conversions / total_customers
    
    # Sum of attributed conversions should equal total converting customers
    assert results['attributed_conversions'].sum() == total_conversions
    
    # All success rates should sum to less than or equal to total customers
    # (since each customer can only convert once)
    channel_success_sum = (results['success_rate'] * results['total_appearances']).sum()
    assert channel_success_sum <= total_customers

