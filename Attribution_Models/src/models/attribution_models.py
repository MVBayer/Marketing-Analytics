from sqlalchemy import text
from abc import ABC, abstractmethod
from ..database.connection import DatabaseConnection
import pandas as pd

class AttributionModel(ABC):
    """Base class for attribution models."""
    
    def __init__(self, db_connection):
        self.db = db_connection
    
    @abstractmethod
    def calculate_attribution_weights(self, config):
        """Calculate attribution weights based on the given configuration."""
        pass
    
    def get_touchpoint_metrics(self, weight_query):
        """Get base metrics for touchpoints using specified weight calculation."""
        base_query = """
        WITH TouchpointWeights AS (
            {weight_calculation}
        ),
        ConversionCount AS (
            -- Get total number of conversions (each customer can have at most one conversion)
            SELECT COUNT(DISTINCT customer_id) as total_conversions
            FROM touchpoints
            WHERE is_conversion = TRUE
        ),
        ChannelStats AS (
            -- Calculate per-channel statistics
            SELECT 
                t.touchpoint as channel,
                COUNT(DISTINCT t.customer_id) as total_appearances,
                SUM(t.channel_cost) as total_cost
            FROM touchpoints t
            WHERE t.is_conversion = FALSE
            GROUP BY t.touchpoint
        ),
        -- Get conversion values
        ConversionValues AS (
            SELECT 
                customer_id,
                MAX(purchase_value) as value
            FROM touchpoints
            WHERE is_conversion = TRUE
            GROUP BY customer_id
        ),
        -- Attribution calculations
        ChannelAttributions AS (
            SELECT
                cs.channel,
                -- Count distinct customers with conversions that had this touchpoint
                COUNT(DISTINCT tw.customer_id) as customers_with_conversions,
                -- Sum the attribution weights for this channel
                SUM(tw.weight) as attribution_weight,
                -- Calculate attributed revenue (weight * conversion value)
                SUM(tw.weight * cv.value) as attributed_revenue,
                -- Sum of all conversion values
                SUM(cv.value) as total_conversion_value
            FROM ChannelStats cs
            LEFT JOIN TouchpointWeights tw ON cs.channel = tw.touchpoint
            LEFT JOIN ConversionValues cv ON tw.customer_id = cv.customer_id
            GROUP BY cs.channel
        ),
        -- Get total attribution weight for normalization
        TotalAttributionWeight AS (
            SELECT SUM(attribution_weight) as total_weight
            FROM ChannelAttributions
            WHERE attribution_weight IS NOT NULL
        ),
        -- Get total conversion value
        TotalConversionValue AS (
            SELECT SUM(purchase_value) as total_value
            FROM touchpoints
            WHERE is_conversion = TRUE
        )
        -- Final metrics
        SELECT
            cs.channel,
            -- Attribution percentage (normalized by total weight to ensure sum to 1.0)
            COALESCE(ca.attribution_weight / NULLIF((SELECT total_weight FROM TotalAttributionWeight), 0), 0) 
                as attribution_percentage,
            -- Number of attributed conversions
            COALESCE(ca.attribution_weight, 0) as attributed_conversions,
            -- Total customer touchpoints with this channel
            cs.total_appearances,
            -- Success rate (customers with conversions / total appearances)
            COALESCE(ca.customers_with_conversions / CAST(cs.total_appearances AS FLOAT), 0) as success_rate,
            -- Total cost for this channel
            cs.total_cost,
            -- Attributed revenue (normalized to match total purchase value)
            CASE 
                WHEN (SELECT SUM(attributed_revenue) FROM ChannelAttributions) > 0 
                THEN COALESCE(ca.attributed_revenue, 0) * ((SELECT total_value FROM TotalConversionValue) / 
                    (SELECT SUM(attributed_revenue) FROM ChannelAttributions))
                ELSE 0
            END as attributed_revenue,
            -- Return on investment
            CASE 
                WHEN cs.total_cost > 0 THEN (COALESCE(ca.attributed_revenue, 0) - cs.total_cost) / cs.total_cost
                ELSE 0
            END as roi
        FROM ChannelStats cs
        LEFT JOIN ChannelAttributions ca ON cs.channel = ca.channel
        ORDER BY attribution_percentage DESC
        """
        return base_query.format(weight_calculation=weight_query)

class SingleTouchAttribution(AttributionModel):
    """Single-touch attribution model."""
    
    ATTRIBUTION_TYPES = {
        'first': {'order': 'ASC', 'window_clause': '1'},
        'last': {'order': 'DESC', 'window_clause': '1'},
        'second_to_last': {'order': 'DESC', 'window_clause': '2'},
        'third_to_last': {'order': 'DESC', 'window_clause': '3'}
    }
    
    def calculate_attribution_weights(self, attribution_type='first'):
        """Calculate weights for single-touch attribution."""
        if attribution_type not in self.ATTRIBUTION_TYPES:
            raise ValueError(f"Invalid attribution type: {attribution_type}. Expected one of {list(self.ATTRIBUTION_TYPES.keys())}")
            
        config = self.ATTRIBUTION_TYPES[attribution_type]
        
        # For single-touch attribution, we assign a weight of 1.0 to the specified touchpoint
        # and 0 to all others
        weight_query = f"""
        -- Get the {attribution_type} non-converting touchpoint for each customer
        SELECT 
            t.customer_id,
            t.touchpoint,
            1.0 as weight  -- Full attribution to the {attribution_type} touchpoint
        FROM (
            SELECT
                customer_id,
                touchpoint,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id 
                    ORDER BY timestamp {config['order']}
                ) as touch_order
            FROM touchpoints
            WHERE is_conversion = FALSE
        ) t
        -- Only include customers who converted
        WHERE t.touch_order = {config['window_clause']}
        AND EXISTS (
            SELECT 1 FROM touchpoints 
            WHERE customer_id = t.customer_id AND is_conversion = TRUE
        )
        """
        return weight_query
    
    def calculate_channel_metrics(self, attribution_type='first'):
        """Calculate channel metrics using the specified attribution type."""
        weight_query = self.calculate_attribution_weights(attribution_type)
        metrics_query = self.get_touchpoint_metrics(weight_query)
        
        with self.db.get_session() as session:
            result = pd.read_sql(metrics_query, session.bind)
            return result.set_index('channel')
        
class MultiTouchAttribution(AttributionModel):
    """Multi-touch attribution model implementation."""
    
    # Define standard multi-touch attribution models
    ATTRIBUTION_MODELS = {
        'u_shaped': {'first': 0.4, 'last': 0.4, 'middle': 0.2},
        'w_shaped': {'first': 0.3, 'last': 0.3, 'middle': 0.4},
        'linear': 'linear',
        'time_decay': 'time_decay'
    }
    
    def calculate_attribution_weights(self, model_type):
        """Calculate weights for multi-touch attribution."""
        # Handle predefined models
        if isinstance(model_type, str) and model_type in self.ATTRIBUTION_MODELS:
            model_config = self.ATTRIBUTION_MODELS[model_type]
        else:
            model_config = model_type
            
        # Handle U-shaped or other position-based models
        if isinstance(model_config, dict) and 'first' in model_config:
            # Direct implementation of U-shaped attribution
            return self.get_u_shaped_weight_query(
                first_weight=model_config.get('first', 0.4),
                last_weight=model_config.get('last', 0.4),
                middle_weight=model_config.get('middle', 0.2)
            )
        elif model_config == 'linear':
            return self.get_linear_weight_query()
        elif model_config == 'time_decay':
            return self.get_time_decay_weight_query()
        else:
            raise ValueError(f"Unsupported attribution model: {model_type}")
    
    def get_u_shaped_weight_query(self, first_weight=0.4, last_weight=0.4, middle_weight=0.2):
        """Generate a query for U-shaped attribution model."""
        return f"""
        -- U-shaped attribution weights
        WITH CustomerJourneys AS (
            -- Get journey details for each customer who converted
            SELECT
                t.customer_id,
                t.touchpoint,
                t.timestamp,
                ROW_NUMBER() OVER (PARTITION BY t.customer_id ORDER BY t.timestamp ASC) as position,
                COUNT(*) OVER (PARTITION BY t.customer_id) as total_positions
            FROM touchpoints t
            WHERE t.is_conversion = FALSE
            AND EXISTS (
                SELECT 1 FROM touchpoints 
                WHERE customer_id = t.customer_id AND is_conversion = TRUE
            )
        )
        -- Apply U-shaped weights
        SELECT
            customer_id,
            touchpoint,
            CASE
                -- First touchpoint
                WHEN position = 1 THEN {first_weight}
                
                -- Last touchpoint
                WHEN position = total_positions THEN {last_weight}
                
                -- Middle touchpoints (distribute remaining weight)
                WHEN total_positions > 2 THEN {middle_weight} / (total_positions - 2)
                
                -- Edge case: No middle touchpoints
                ELSE 0
            END as weight
        FROM CustomerJourneys
        """
    
    def get_linear_weight_query(self):
        """Generate a query for linear attribution model."""
        return """
        -- Linear attribution weights
        WITH CustomerJourneys AS (
            -- Count touchpoints per converting customer
            SELECT 
                t.customer_id,
                COUNT(*) as touchpoint_count
            FROM touchpoints t
            WHERE t.is_conversion = FALSE
            AND EXISTS (
                SELECT 1 FROM touchpoints 
                WHERE customer_id = t.customer_id AND is_conversion = TRUE
            )
            GROUP BY t.customer_id
        )
        -- Equal weight to all touchpoints in a customer journey
        SELECT
            t.customer_id,
            t.touchpoint,
            1.0 / CAST(cj.touchpoint_count AS FLOAT) as weight
        FROM touchpoints t
        JOIN CustomerJourneys cj ON t.customer_id = cj.customer_id
        WHERE t.is_conversion = FALSE
        """
    
    def get_time_decay_weight_query(self, half_life=7):
        """Generate a query for time decay attribution model."""
        return f"""
        -- Time decay attribution weights
        WITH ConversionTimes AS (
            -- Get conversion time for each customer
            SELECT
                customer_id,
                MIN(timestamp) as conversion_time
            FROM touchpoints
            WHERE is_conversion = TRUE
            GROUP BY customer_id
        ),
        TouchpointDecays AS (
            -- Calculate decay for each touchpoint
            SELECT
                t.customer_id,
                t.touchpoint,
                t.timestamp,
                -- Days before conversion
                MAX(0, CAST(julianday(ct.conversion_time) - julianday(t.timestamp) AS FLOAT)) as days_before,
                -- Decay factor: 2^(-days/half_life) - higher for more recent touchpoints
                POWER(2, -MAX(0, CAST(julianday(ct.conversion_time) - julianday(t.timestamp) AS FLOAT)) / {half_life}) as decay_factor
            FROM touchpoints t
            JOIN ConversionTimes ct ON t.customer_id = ct.customer_id
            WHERE t.is_conversion = FALSE
            AND EXISTS (
                SELECT 1 FROM touchpoints 
                WHERE customer_id = t.customer_id AND is_conversion = TRUE
            )
            GROUP BY t.customer_id, t.touchpoint, t.timestamp
        ),
        CustomerTotals AS (
            -- Sum of decay factors per customer
            SELECT
                customer_id,
                SUM(decay_factor) as total_decay
            FROM TouchpointDecays
            GROUP BY customer_id
        )
        -- Normalized weights
        SELECT
            td.customer_id,
            td.touchpoint,
            -- Normalize by customer's total decay
            td.decay_factor / ct.total_decay as weight
        FROM TouchpointDecays td
        JOIN CustomerTotals ct ON td.customer_id = ct.customer_id
        """
    
    def calculate_channel_metrics(self, model_type):
        """Calculate attribution metrics for specified model."""
        try:
            # Generate the weight query based on the model type
            weight_query = self.calculate_attribution_weights(model_type)
            
            # Use the base metrics query
            metrics_query = self.get_touchpoint_metrics(weight_query)
            
            # Execute the query and return the results
            with self.db.get_session() as session:
                result = pd.read_sql(metrics_query, session.bind)
                # Ensure proper index
                if 'channel' in result.columns:
                    return result.set_index('channel')
                return result
        except Exception as e:
            print(f"Error in calculate_channel_metrics: {str(e)}")
            raise