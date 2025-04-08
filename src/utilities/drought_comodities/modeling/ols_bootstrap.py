# 02_modeling/ols_bootstrap.py

import numpy as np
import statsmodels.api as sm

def run_regression_bootstrap(series, spei_series, n_boot=1000):
    """
    Runs an OLS regression of the series on a SPEI variable and applies bootstrapping for robust inference.
    
    Parameters:
        series (array-like): Time series of the dependent variable (e.g., coefficients of a spectral band).
        spei_series (array-like): Time series of the exogenous variable (e.g., SPEI on the same scale).
        n_boot (int): Number of bootstrapping samples.
    
    Returns:
        tuple: (original coefficients, mean of bootstrap coefficients, bootstrap standard deviation)
    """
    # Prepare the data: add constant for intercept
    X = np.array(spei_series)
    y = np.array(series)
    X = sm.add_constant(X)
    
    # Fit the OLS model on the original sample
    model = sm.OLS(y, X).fit()
    original_coef = model.params
    
    # Bootstrap coefficients
    boot_coefs = []
    n = len(y)
    for _ in range(n_boot):
        # Sampling with replacement
        indices = np.random.choice(n, size=n, replace=True)
        X_boot = X[indices]
        y_boot = y[indices]
        model_boot = sm.OLS(y_boot, X_boot).fit()
        boot_coefs.append(model_boot.params)
    
    boot_coefs = np.array(boot_coefs)
    boot_mean = boot_coefs.mean(axis=0)
    boot_std = boot_coefs.std(axis=0)
    
    return original_coef, boot_mean, boot_std

def run_regression_bootstrap_on_components(spectral_components, spei_components, n_boot=1000):
    """
    Applies OLS regressions with bootstrapping to each spectral component of the series and the SPEI variable.
    
    Parameters:
        spectral_components (dict): Dictionary of series for each scale (e.g., {'j=1': data, ...}).
        spei_components (dict): Dictionary of SPEI series corresponding to each scale.
        n_boot (int): Number of iterations for bootstrapping.
        
    Returns:
        dict: Results with keys by scale and values that include original coefficients, bootstrap mean, and bootstrap standard deviation.
    """
    results = {}
    for key in spectral_components:
        if key in spei_components:
            series = spectral_components[key]
            spei_series = spei_components[key]
            coef, boot_mean, boot_std = run_regression_bootstrap(series, spei_series, n_boot)
            results[key] = {
                'coef': coef,
                'boot_mean': boot_mean,
                'boot_std': boot_std
            }
    return results

def run_regression_bootstrap_demo():
    """
    Demonstration function: simulates data for spectral components and SPEI variables, and runs the bootstrapping.
    """
    # Create simulated data to demonstrate functionality (100 observations)
    dummy_series = np.random.randn(100)
    dummy_spei = np.random.randn(100)
    
    # Simulate components for 6 scales
    spectral_components = {f'j={j}': dummy_series for j in range(1, 7)}
    spei_components = {f'j={j}': dummy_spei for j in range(1, 7)}
    
    results = run_regression_bootstrap_on_components(spectral_components, spei_components, n_boot=100)
    return results

if __name__ == "__main__":
    results_demo = run_regression_bootstrap_demo()
    for key, res in results_demo.items():
        print(f"{key}: coef = {res['coef']}, boot_mean = {res['boot_mean']}, boot_std = {res['boot_std']}")
