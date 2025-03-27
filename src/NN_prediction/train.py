"""
Module for training the model.
"""

from tensorflow.keras.callbacks import ModelCheckpoint
from NN_prediction.model import build_ensemble_model
from NN_prediction import config


def train_model(X_climate_train, X_yield_train, y_train, model_save_path: str = "ensemble_model.h5"):
    """
    Train the ensemble model using the provided training data.
    Saves the best model based on the validation loss.
    """
    model = build_ensemble_model()
    model.summary()  # Print model architecture

    checkpoint = ModelCheckpoint(
        model_save_path, monitor="val_loss", verbose=1, save_best_only=True, mode="min"
    )

    history = model.fit(
        {"climate_input": X_climate_train, "yield_input": X_yield_train},
        y_train,
        epochs=config.EPOCHS,
        batch_size=config.BATCH_SIZE,
        validation_split=0.3,
        callbacks=[checkpoint],
        verbose=1,
    )
    return model, history
