"""
Module for defining the ensemble CNN-RNN with LSTM model.
"""

from tensorflow.keras.models import Model
from tensorflow.keras.layers import (
    Input,
    Conv1D,
    MaxPooling1D,
    Flatten,
    LSTM,
    Dense,
    Dropout,
    concatenate,
)
from tensorflow.keras.optimizers import Adam
from NN_prediction import config


def build_ensemble_model() -> Model:
    """
    Build the ensemble CNN-RNN with LSTM model.
    Two branches:
      - CNN branch for processing climate data.
      - RNN branch (LSTM) for processing yield data.
    The outputs are concatenated and passed through Dense layers.
    """
    # Define input shapes:
    # Climate input: (TIME_STEP, NUM_CLIMATE_FEATURES)
    input_climate = Input(shape=(config.TIME_STEP, config.NUM_CLIMATE_FEATURES), name="climate_input")
    # Yield input: (TIME_STEP, NUM_YIELD_FEATURES)
    input_yield = Input(shape=(config.TIME_STEP, config.NUM_YIELD_FEATURES), name="yield_input")

    # CNN branch for climate data
    x = Conv1D(filters=config.CNN_FILTERS, kernel_size=config.CNN_KERNEL_SIZE, activation="relu")(input_climate)
    x = MaxPooling1D(pool_size=config.POOL_SIZE)(x)
    x = Flatten()(x)

    # RNN branch for yield data (using LSTM layers)
    y = LSTM(config.LSTM_UNITS, return_sequences=True)(input_yield)
    y = LSTM(config.LSTM_UNITS)(y)

    # Concatenate the features from both branches
    combined = concatenate([x, y])
    z = Dense(config.DENSE_UNITS, activation="relu")(combined)
    z = Dropout(config.DROPOUT)(z)
    output = Dense(1, name="yield_prediction")(z)

    model = Model(inputs=[input_climate, input_yield], outputs=output)
    model.compile(
        optimizer=Adam(learning_rate=config.LEARNING_RATE),
        loss="mse",
        metrics=["mae"],
    )
    return model
