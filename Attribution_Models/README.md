# Marketing Attribution Models

A comprehensive toolkit for analyzing customer journey touchpoints and attributing conversion value across marketing channels.

## Overview

This project implements various attribution models used in marketing analytics to evaluate the effectiveness of different touchpoints in the customer journey. These models help marketers understand which channels and interactions contribute most to conversions and revenue.

## Attribution Models Implemented

### Single-Touch Attribution
* **First Touch**: Credits the first touchpoint in a customer journey
* **Last Touch**: Credits the last touchpoint before conversion
* **Second-to-Last Touch**: Credits the second-to-last touchpoint
* **Third-to-Last Touch**: Credits the third-to-last touchpoint

### Multi-Touch Attribution
* **Linear**: Distributes credit equally across all touchpoints
* **U-Shaped (Position-Based)**: Gives 40% credit to first and last touchpoints, with 20% distributed among middle touchpoints
* **W-Shaped**: Gives 30% credit to first and last touchpoints, with 40% distributed among middle touchpoints
* **Time Decay**: Gives more credit to touchpoints closer to conversion

## Project Structure

```
attribution-models/
├── src/                      # Source code
│   ├── database/             # Database connection and management
│   ├── models/               # Attribution model implementations
│   ├── utils/                # Utility functions
│   ├── config.py             # Configuration settings
│   └── main.py               # Main application entry point
├── tests/                    # Unit and integration tests
│   ├── test_multi_touch.py   # Tests for multi-touch models
│   └── test_single_touch.py  # Tests for single-touch models
├── data/                     # Data storage
│   ├── raw/                  # Raw input data
│   ├── processed/            # Processed data
│   ├── sql/                  # SQL queries
│   └── attribution.db        # SQLite database
├── z_learning_resources/     # Documentation and references
├── requirements.txt          # Project dependencies
├── setup.py                  # Package configuration
└── README.md                 # Project documentation
```

## Technical Implementation

* **Database**: SQLite with SQLAlchemy ORM
* **Analysis**: Custom SQL queries for attribution calculations
* **Testing**: Unittest framework with in-memory database testing

## Installation

1. Clone the repository:
```bash
git clone https://github.com/MVBayer/Marketing-Analytics.git
cd Marketing-Analytics
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up environment variables (copy from example):
```bash
cp .env.example .env
```

## Usage

### Running the Analysis

```python
from src.models.attribution_models import SingleTouchAttribution, MultiTouchAttribution
from src.database.connection import DatabaseConnection

# Initialize database connection
db = DatabaseConnection()

# Single-touch attribution
single_touch = SingleTouchAttribution(db)
first_touch_results = single_touch.calculate_channel_metrics('first')

# Multi-touch attribution
multi_touch = MultiTouchAttribution(db)
u_shaped_results = multi_touch.calculate_channel_metrics('u_shaped')
```

## Example Output

The models produce attribution metrics for each marketing channel, including:
* Attribution percentage
* Attributed revenue
* Return on investment (ROI)
* Cost per acquisition (CPA)

## Future Development

* Markov Chain attribution models
* Machine learning-based attribution
* Interactive visualization dashboard
* Enhanced reporting capabilities

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any suggestions or improvements.

## License

This project is licensed under the MIT License.
```


