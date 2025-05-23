# 📈 Deep Learning Portfolio Optimization

A **Python-based implementation** of the research paper "Deep-Learning-for-Portfolio-Optimization" that provides advanced tools for portfolio optimization using both traditional and modern approaches:

1. **Deep Learning Strategy (DLS)** - A modern deep learning approach using LSTM networks
2. **Fixed Allocation Strategies** - Traditional portfolio allocation methods

This project is designed for financial analysts, researchers, and students interested in exploring both traditional and cutting-edge approaches to portfolio optimization.

---

## 📒 Table of Contents

1. [Features](#-features)
2. [Technology Stack](#-technology-stack)
3. [Installation](#-installation)
4. [Usage](#-usage)
5. [Project Structure](#-project-structure)
6. [Models Overview](#-models-overview)
7. [Performance Metrics](#-performance-metrics)
8. [Contributing](#-contributing)
9. [Contact](#-contact)

---

## 🌟 Features

- **Deep Learning Model**: LSTM-based neural network for dynamic portfolio allocation
- **Multiple Asset Classes**: Support for stocks (VTI), bonds (AGG), commodities (DBC), and volatility (VIX)
- **Fixed Allocation Strategies**: Four different static allocation methods for comparison
- **Performance Analysis**: Comprehensive evaluation metrics including Sharpe ratio, Sortino ratio, and drawdown analysis
- **Volatility Targeting**: Dynamic portfolio scaling based on market volatility
- **Transaction Cost Modeling**: Realistic simulation including trading costs

---

## 🛠️ Technology Stack

- **Python**: Core programming language
- **TensorFlow**: Deep learning framework for LSTM implementation
- **NumPy**: For numerical computations
- **Pandas**: For data manipulation and analysis
- **Matplotlib**: For data visualization
- **Jupyter Notebooks**: For interactive development and analysis

---

## 📦 Installation

1. **Clone the Repository**

```bash
git clone <your-repository-url>
cd <repository-name>
```

2. **Create a Virtual Environment (Recommended)**

```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

3. **Install Required Packages**

```bash
pip install -r requirements.txt
```

---

## 🔍 Usage

1. **Data Preprocessing**
   - Run `data_preprocessing.ipynb` to prepare the historical data
   - This will create the necessary input features and target returns

2. **Model Training and Evaluation**
   - Run `Neural_network.ipynb` to train the deep learning model
   - The model will be trained on historical data and evaluated on test data

3. **Fixed Allocation Analysis**
   - Run `fixed_allocation_algo.ipynb` to evaluate traditional allocation strategies
   - Compare performance metrics with the deep learning approach

---

## 🔄 Project Structure

```plaintext
project/
│
├── data_preprocessing.ipynb     # Data preparation and feature engineering
├── Neural_network.ipynb         # Deep learning model implementation
├── fixed_allocation_algo.ipynb  # Traditional allocation strategies
├── requirements.txt            # Python dependencies
└── README.md                  # Project documentation
```

---

## 📈 Models Overview

### 1. **Deep Learning Strategy (DLS)**
A modern approach using LSTM networks for dynamic portfolio allocation.

- **Key Features:**
  - LSTM-based architecture for temporal pattern recognition
  - Dynamic weight allocation based on market conditions
  - Volatility targeting for risk management
  - Transaction cost consideration

### 2. **Fixed Allocation Strategies**
Traditional portfolio allocation methods with four different configurations:

- **Allocation 1**: Equal weights (25% each)
- **Allocation 2**: Equity-focused (50% stocks)
- **Allocation 3**: Bond-focused (50% bonds)
- **Allocation 4**: Balanced (40% stocks, 40% bonds)

---

## 📊 Performance Metrics

The project evaluates portfolio performance using multiple metrics:

- **Annualized Return**: Expected annual portfolio return
- **Annualized Volatility**: Portfolio risk measure
- **Sharpe Ratio**: Risk-adjusted return metric
- **Sortino Ratio**: Downside risk-adjusted return
- **Maximum Drawdown**: Worst peak-to-trough decline
- **Win Rate**: Percentage of positive returns
- **Profit/Loss Ratio**: Average gain to average loss ratio

---

## 💪 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add new feature'`)
5. Push to the branch (`git push origin feature-branch`)
6. Create a Pull Request

---

## 📧 Contact

[Your Name]  
[Your GitHub Username] | [Your GitHub Profile URL]

---

> This project is designed for educational and research purposes. The deep learning model represents an experimental approach to portfolio optimization and should be used with appropriate caution in real-world applications. Past performance is not indicative of future results.

This README provides a comprehensive overview of your project, highlighting its key features, implementation details, and usage instructions. The structure follows the example you provided while being specifically tailored to your portfolio optimization project. You can customize the contact information and repository URLs as needed.
