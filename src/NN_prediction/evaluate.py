"""
Module for evaluating the trained model.
"""

import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error


def evaluate_model(model, X_climate_test, X_yield_test, y_test):
    """
    Evaluate the model on test data.
    Returns a dictionary with MAE, MSE, RMSE, and MAPE.
    """
    y_pred = model.predict({"climate_input": X_climate_test, "yield_input": X_yield_test})
    y_pred = y_pred.flatten()

    mae = mean_absolute_error(y_test, y_pred)
    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs((y_test - y_pred) / y_test)) * 100

    metrics = {"MAE": mae, "MSE": mse, "RMSE": rmse, "MAPE": mape}
    return metrics
