"""
Main entry point for replicating the paper's results.
This script loads the data, builds and trains the model, and evaluates its performance.
"""

from NN_prediction import data, train, evaluate, utils

def main():
    # Paths to your data files
    climate_csv = "data/climate.csv"  # Replace with your actual file path
    yield_csv = "data/yield.csv"      # Replace with your actual file path

    # Load and preprocess the data
    (
        X_climate_train,
        X_yield_train,
        y_train,
        X_climate_test,
        X_yield_test,
        y_test,
    ) = data.load_and_preprocess(climate_csv, yield_csv)

    # Train the ensemble model
    model, history = train.train_model(X_climate_train, X_yield_train, y_train)

    # Evaluate the model on test data
    metrics = evaluate.evaluate_model(model, X_climate_test, X_yield_test, y_test)
    print("Evaluation Metrics:")
    for key, value in metrics.items():
        print(f"{key}: {value:.4f}")

    # Optionally, plot predictions
    y_pred = model.predict({"climate_input": X_climate_test, "yield_input": X_yield_test})
    utils.plot_predictions(y_test, y_pred.flatten())

if __name__ == "__main__":
    main()
