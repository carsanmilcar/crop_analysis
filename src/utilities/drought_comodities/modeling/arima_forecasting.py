# 02_modeling/arima_forecasting.py

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA

def run_arima_forecasting(series, exog=None, order=(1, 0, 1), forecast_steps=12):
    """
    Ajusta un modelo ARIMA (o ARIMAX si se proporciona 'exog') y realiza un pronóstico.
    
    Parameters:
        series (pandas.Series o array-like): Serie temporal de entrenamiento.
        exog (pandas.Series o array-like, opcional): Variable(s) exógena(s) para ARIMAX.
        order (tuple): Orden del modelo ARIMA (p, d, q).
        forecast_steps (int): Número de períodos a pronosticar.
    
    Returns:
        tuple: (modelo ajustado, pronóstico para forecast_steps períodos)
    """
    if exog is not None:
        # Asegurarse de que exog es un DataFrame (o se trate adecuadamente)
        model = ARIMA(series, order=order, exog=exog).fit()
        # Para pronosticar se requiere proveer valores exógenos para el período de pronóstico.
        # En este ejemplo, usamos los últimos 'forecast_steps' valores de exog.
        forecast_exog = exog[-forecast_steps:]
        forecast = model.forecast(steps=forecast_steps, exog=forecast_exog)
    else:
        model = ARIMA(series, order=order).fit()
        forecast = model.forecast(steps=forecast_steps)
    return model, forecast

def run_forecasting_demo():
    """
    Función de demostración que crea series temporales ficticias y ejecuta modelos ARIMA y ARIMAX.
    """
    np.random.seed(42)
    # Simular una serie de 100 puntos
    series = pd.Series(np.random.randn(100))
    # Crear una variable exógena ficticia (por ejemplo, una serie SPEI simulada)
    spei = pd.Series(np.random.randn(100))
    forecast_steps = 12
    order = (1, 0, 1)
    
    # Ajustar modelo ARIMA (sin exógena)
    model_arima, forecast_arima = run_arima_forecasting(series, order=order, forecast_steps=forecast_steps)
    
    # Ajustar modelo ARIMAX (con SPEI como exógena)
    model_arimax, forecast_ar
