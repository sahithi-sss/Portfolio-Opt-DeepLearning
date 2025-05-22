import numpy as np
import tensorflow as tf

X = np.load('X_data.npy')
R = np.load('Y_data.npy')
# X` is the preprocessed input data of shape (3492, 50, 8)
# corresponding asset returns `R` of shape (3492, 4)
# Each row contains [r_stock, r_bond, r_commodity, r_volatility]
assert X.shape[0] == R.shape[0]

n_assets = R.shape[1]
lookback = X.shape[1]

# -----------------------------
# Model definition
# -----------------------------
class SharpeModel(tf.keras.Model):
    def __init__(self, n_assets):
        super().__init__()
        self.lstm = tf.keras.layers.LSTM(64)
        self.dense = tf.keras.layers.Dense(n_assets)

    def call(self, x):
        x = self.lstm(x)
        raw_weights = self.dense(x)
        weights = tf.nn.softmax(raw_weights)  # Eq. (5)
        return weights

model = SharpeModel(n_assets)

# -----------------------------
# Custom Loss Function (Eq. 1-6)
# -----------------------------
def sharpe_loss(R, W):
    # R: shape (batch, n_assets) -- returns = the actual returns of each asset for a given time step t
    # W: shape (batch, n_assets) -- weights = predicted portfolio weights from the model
    portfolio_returns = tf.reduce_sum(R * W, axis=1)  # Eq. (3)
    mean = tf.reduce_mean(portfolio_returns)          # Eq. (2)
    std = tf.math.reduce_std(portfolio_returns)
    sharpe = mean / (std + 1e-8)                      # Eq. (1)
    return -sharpe  # Negative because we minimize in TF

# -----------------------------
# Training loop
# -----------------------------
optimizer = tf.keras.optimizers.Adam(learning_rate=1e-3)

batch_size = 64
epochs = 100
val_split = 0.1

X_train, R_train = X[:-int(len(X)*val_split)], R[:-int(len(X)*val_split)]
X_val, R_val = X[-int(len(X)*val_split):], R[-int(len(X)*val_split):]

train_dataset = tf.data.Dataset.from_tensor_slices((X_train, R_train)).batch(batch_size)

for epoch in range(epochs):
    for batch_x, batch_r in train_dataset:
        with tf.GradientTape() as tape:
            weights = model(batch_x)
            loss = sharpe_loss(batch_r, weights)
        grads = tape.gradient(loss, model.trainable_variables)
        optimizer.apply_gradients(zip(grads, model.trainable_variables))
    print(f"Epoch {epoch+1}: Loss = {-loss.numpy():.4f} (Sharpe)")

# ------------------------------
# Volatility Estimate & Backtest (Eq. 7)
# ------------------------------
def exponential_weighted_volatility(returns, span=50):
    alpha = 2 / (span + 1)
    ewma = [returns[0]]
    for r in returns[1:]:
        ewma.append(alpha * r + (1 - alpha) * ewma[-1])
    ewma = np.array(ewma)
    squared_diff = (returns - ewma) ** 2
    ew_var = np.convolve(squared_diff, np.exp(-np.arange(span)[::-1] * alpha), mode='valid')
    ew_std = np.sqrt(ew_var)
    return np.pad(ew_std, (span - 1, 0), constant_values=np.nan)

def backtest(X, R, model, cost_rate=0.0001, vol_target=0.10):
    weights = []
    returns = []
    prev_scaled_weights = np.zeros(R.shape[1])
    vol_estimates = np.array([exponential_weighted_volatility(R[:, i]) for i in range(R.shape[1])]).T

    for t in range(2, len(X)):
        x_input = X[t:t+1]
        w = model(x_input).numpy().flatten()
        sigma = vol_estimates[t - 1]
        if np.any(np.isnan(sigma)):
            returns.append(0)
            continue
        scaled_w = (vol_target / sigma) * w
        rp = np.dot(scaled_w, R[t])
        tc = cost_rate * np.sum(np.abs(scaled_w - prev_scaled_weights))
        net_return = rp - tc
        returns.append(net_return)
        prev_scaled_weights = scaled_w
        weights.append(w)

    return np.array(returns), np.array(weights)

# ------------------------------
# Rolling Re-training & Backtesting
# ------------------------------
def rolling_train_test(X, R, train_start=0, initial_train_len=1279, retrain_window=504):
    all_returns = []
    all_weights = []

    current_train_end = initial_train_len

    while current_train_end + retrain_window <= len(X):
        print(f"\n📦 Training on samples 0 to {current_train_end - 1}")
        model = SharpeModel(n_assets=R.shape[1])
        optimizer = tf.keras.optimizers.Adam(1e-3)

        # Training
        train_dataset = tf.data.Dataset.from_tensor_slices(
            (X[:current_train_end], R[:current_train_end])
        ).batch(64)

        for epoch in range(20):  # use fewer epochs for rolling to be fast
            for batch_x, batch_r in train_dataset:
                with tf.GradientTape() as tape:
                    weights = model(batch_x)
                    loss = sharpe_loss(batch_r, weights)
                grads = tape.gradient(loss, model.trainable_variables)
                optimizer.apply_gradients(zip(grads, model.trainable_variables))

        # Backtest on next period
        X_test_window = X[current_train_end : current_train_end + retrain_window]
        R_test_window = R[current_train_end : current_train_end + retrain_window]
        print(f"📊 Backtesting from sample {current_train_end} to {current_train_end + retrain_window - 1}")
        window_returns, window_weights = backtest(X_test_window, R_test_window, model)

        all_returns.append(window_returns)
        all_weights.append(window_weights)

        current_train_end += retrain_window

    return np.concatenate(all_returns), all_weights

# ------------------------------
# Run It
# ------------------------------

# Make sure X and R are loaded/preprocessed
# X: shape (3492, 50, 8), R: shape (3492, 4)
initial_train_len = 1279  # based on April 3, 2006 to April 30, 2011
retrain_window = 504      # ~2 years of daily data (252 days/year)

final_returns, final_weights = rolling_train_test(X, R, initial_train_len=initial_train_len, retrain_window=retrain_window)

# Performance
sharpe = np.mean(final_returns) / (np.std(final_returns) + 1e-8) * np.sqrt(252)
print(f"\n✅ Final annualized Sharpe ratio: {sharpe:.3f}")