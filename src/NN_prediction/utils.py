"""
Utility functions for NN_prediction module.
"""

import matplotlib.pyplot as plt


def plot_predictions(y_true, y_pred, title: str = "Actual vs Predicted Yield"):
    """
    Plot the actual vs. predicted cocoa yield.
    """
    plt.figure(figsize=(8, 6))
    plt.plot(y_true, label="Actual Yield", marker="o")
    plt.plot(y_pred, label="Predicted Yield", marker="x")
    plt.title(title)
    plt.xlabel("Sample Index")
    plt.ylabel("Normalized Yield")
    plt.legend()
    plt.tight_layout()
    plt.show()
