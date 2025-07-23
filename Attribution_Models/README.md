# attribution-models/README.md

# Attribution Models Project

This project implements various attribution models used in marketing analytics to evaluate the effectiveness of different touchpoints in the customer journey. The models included in this project are:

- **Single-Touch Attribution Models**: These models attribute all credit for a conversion to a single touchpoint. The project includes implementations for both first and last interaction models.

- **Multi-Touch Attribution Models**: These models distribute credit across multiple touchpoints. Implementations include linear, U-shaped, and time decay models.

- **Algorithmic Attribution Models**: These models utilize machine learning techniques to determine the most influential touchpoints based on historical data.

## Project Structure

```
attribution-models
├── src
│   ├── models
│   ├── utils
│   └── main.py
├── tests
├── data
│   ├── raw
│   └── processed
├── requirements.txt
├── setup.py
└── README.md
```

## Installation

To install the required dependencies, run:

```
pip install -r requirements.txt
```

## Usage

To run the project, execute the following command:

```
python src/main.py
```

This will load the dataset, apply the attribution models, and demonstrate their functionality.

## Dataset

The project uses a custom dataset located in the `data/raw` directory. The dataset contains customer touchpoints that will be processed and analyzed using the implemented attribution models.

## Contributing

Contributions are welcome! Please feel free to submit a pull request or open an issue for any suggestions or improvements.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.