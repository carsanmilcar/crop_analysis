"""
Module for data loading and preprocessing.
Assumes two CSV files: one for the climatic data and one for the yield data.
Each CSV must include a 'year' column to allow merging.
For simplicity, we assume that the climate CSV has columns: 
    "year", "rainfall", "min_temp", "max_temp"
and the yield CSV has columns:
    "year", "yield".
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from NN_prediction import config


def load_and_merge_data(climate_path: str, yield_path: str) -> pd.DataFrame:
    """Load climate and yield data, merge on 'year', and sort by year."""
    df_climate = pd.read_csv(climate_path)
    df_yield = pd.read_csv(yield_path)
    df = pd.merge(df_climate, df_yield, on="year", how="inner")
    df.sort_values("year", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def normalize_data(df: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """Apply min–max normalization to the specified columns."""
    scaler = MinMaxScaler()
    df[feature_cols] = scaler.fit_transform(df[feature_cols])
    return df


def create_sequences(data: np.ndarray, time_step: int) -> (np.ndarray, np.ndarray):
    """
    Create sequences from a 2D array.
    Each sequence is of length `time_step`, and the target is the value immediately after the sequence.
    """
    X, y = [], []
    for i in range(len(data) - time_step):
        X.append(data[i : i + time_step])
        y.append(data[i + time_step])
    return np.array(X), np.array(y)


def load_and_preprocess(climate_path: str, yield_path: str, test_split: float = 0.3):
    """
    Load, merge, normalize, and create time-series sequences.
    Returns:
        X_climate_train, X_yield_train, y_train,
        X_climate_test, X_yield_test, y_test
    """
    df = load_and_merge_data(climate_path, yield_path)

    # Normalize climate features and yield column separately.
    climate_cols = ["rainfall", "min_temp", "max_temp"]
    df = normalize_data(df, climate_cols)
    yield_cols = ["year", "yield"]
    df[yield_cols] = normalize_data(df, yield_cols)

    # Prepare data arrays
    # For climate, we use the three features; for yield, we use year and yield.
    climate_data = df[climate_cols].values
    yield_data = df[["year", "yield"]].values

    # Create sequences with a sliding window of TIME_STEP.
    X_climate, _ = create_sequences(climate_data, config.TIME_STEP)
    X_yield, y = create_sequences(yield_data, config.TIME_STEP)

    # Ensure that X_climate and X_yield have the same number of samples.
    min_samples = min(len(X_climate), len(X_yield))
    X_climate = X_climate[:min_samples]
    X_yield = X_yield[:min_samples]
    y = y[:min_samples, 1]  # Use the yield column as target

    # Split into training and testing sets.
    split_idx = int(len(X_climate) * (1 - test_split))
    X_climate_train = X_climate[:split_idx]
    X_yield_train = X_yield[:split_idx]
    y_train = y[:split_idx]

    X_climate_test = X_climate[split_idx:]
    X_yield_test = X_yield[split_idx:]
    y_test = y[split_idx:]

    return X_climate_train, X_yield_train, y_train, X_climate_test, X_yield_test, y_test
