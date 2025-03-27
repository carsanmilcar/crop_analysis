"""
Configuration file for the NN_prediction module.
"""

# Training hyperparameters
EPOCHS = 200
BATCH_SIZE = 20
LEARNING_RATE = 0.0005
DROPOUT = 0.2
TIME_STEP = 20

# Data parameters
NUM_CLIMATE_FEATURES = 3  # e.g., rainfall, min_temp, max_temp
NUM_YIELD_FEATURES = 2    # e.g., year, yield

# Model architecture parameters
CNN_FILTERS = 64
CNN_KERNEL_SIZE = 3
POOL_SIZE = 2

LSTM_UNITS = 50
DENSE_UNITS = 64
