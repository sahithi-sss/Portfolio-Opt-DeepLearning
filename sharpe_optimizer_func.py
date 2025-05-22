import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.backend import mean, std
from sklearn.model_selection import train_test_split

def prepare_data(price_df, lookback):
    '''
    Prepares sliding window inputs with prices and returns.
    '''
    asset_names = price_df.columns
    prices = price_df.values
    returns = price_df.pct_change().values

    X = []
    for i in range(lookback, len(prices)):
        price_window = prices[i - lookback:i]
        return_window = returns[i - lookback:i]
        combined = np.concatenate([price_window, return_window], axis=1)
        X.append(combined)

    X = np.array(X)
    return X, asset_names

def sharpe_loss(train_data, y_true, y_pred):
    '''
    Custom loss function to maximize Sharpe Ratio
    '''
    price_data = train_data[:, -1]  # only the last price row
    norm_prices = tf.divide(price_data, price_data[0])
    port_values = tf.reduce_sum(norm_prices * y_pred, axis=1)
    port_returns = (port_values[1:] - port_values[:-1]) / port_values[:-1]
    sharpe = mean(port_returns) / (std(port_returns) + 1e-6)  # stability
    return -sharpe

def build_model(input_shape, num_assets):
    '''
    Builds LSTM model to output allocation weights
    '''
    model = Sequential()
    model.add(LSTM(64, input_shape=input_shape))
    model.add(Dense(num_assets, activation='softmax'))
    return model

def train_model(price_df, lookback=50, epochs=100, batch_size=16):
    '''
    Train the model on given price data
    '''
    # Prepare data
    X, asset_names = prepare_data(price_df, lookback)
    y_dummy = np.zeros((X.shape[0], len(asset_names)))

    # Save the last price row (used in loss calculation)
    train_data = tf.cast(tf.constant(X[:, -1, :len(asset_names)]), dtype=tf.float32)

    # Train-validation split
    X_train, X_val, y_train, y_val = train_test_split(X, y_dummy, test_size=0.1, shuffle=False)

    # Build and compile model
    model = build_model(input_shape=(lookback, X.shape[2]), num_assets=len(asset_names))
    
    # Create custom loss function with train_data
    def custom_loss(y_true, y_pred):
        return sharpe_loss(train_data, y_true, y_pred)
    
    model.compile(optimizer='adam', loss=custom_loss)

    # Train the model
    model.fit(
        X_train, y_train,
        validation_data=(X_val, y_val),
        epochs=epochs,
        batch_size=batch_size,
        shuffle=False,
        verbose=1
    )

    return model, asset_names

def get_allocation(model, recent_price_df, lookback):
    '''
    Returns allocation weights based on most recent window
    '''
    if len(recent_price_df) < lookback:
        raise ValueError("Not enough data: need at least {} days".format(lookback))

    prices = recent_price_df.values[-lookback:]
    returns = recent_price_df.pct_change().values[-lookback:]
    combined = np.concatenate([prices, returns], axis=1)
    input_data = combined[np.newaxis, :]
    return model.predict(input_data)[0]

# Example usage:
# model, asset_names = train_model(price_df, lookback=50, epochs=100, batch_size=16)
# allocation = get_allocation(model, recent_price_df, lookback=50) 