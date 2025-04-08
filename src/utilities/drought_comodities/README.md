# Replication of "Impact of Drought on Commodity Market Forecasting"

This module replicates the empirical and forecasting methodology of Kristian Racocha's 2020 thesis at Charles University, which investigates how drought affects agricultural commodity prices using spectral factor models and ARIMAX time series forecasting.

## 📌 Objective

- Decompose the relationship between drought (SPEI-01 index) and commodity prices across different time frequencies.
- Evaluate whether including drought as an exogenous variable improves price forecasts.

## 📂 Project Structure

```
src/utilities/drought-comodities/
├── 00_utils/                # Common helper functions (filters, stationarity tests, plotting)
│   ├── filters.py
│   ├── tests_stationarity.py
│   └── plotting.py
├── 01_data/                 # Scripts for downloading and transforming raw data
│   ├── download_commodity_prices.py
│   ├── process_spei.py
│   └── data_description.md
├── 02_modeling/             # Spectral regression + forecasting models
│   ├── spectral_factor_model.py
│   ├── ols_bootstrap.py
│   └── arima_forecasting.py
├── 03_results/              # Scripts to run models and generate output
│   ├── run_beta_analysis.py
│   ├── run_forecasting.py
│   └── figures/
├── main.py                  # Pipeline entrypoint
└── config.yaml              # Frequency bands, commodity list, parameters
```

## 📉 Methodology Summary

### 1. Data

- **Commodities**: Monthly prices from World Bank (Pink Sheet): wheat, soybeans, maize, cocoa, barley, etc.
- **Drought**: Standardized Precipitation-Evapotranspiration Index (SPEI-01), using Thornthwaite PET method.
- **Returns**: Relative price changes are used instead of raw prices.

### 2. Spectral Factor Models

Based on Bandi et al. (2019), the model decomposes series into components by frequency using Haar wavelets. For each frequency \( j \):

\[
R^{(j)}_t = \alpha + \beta^{(j)} \cdot \text{SPEI}^{(j)}_t + \varepsilon_t
\]

- Frequencies range from 1–2 months (high) to >64 months (low).
- Traditional beta is reconstructed as weighted sum of spectral betas.

### 3. Bootstrapped Inference

- Stationary bootstrap (Politis & Romano, 1994)
- Confidence intervals via Ledoit & Wolf (2008, 2011)
- Outputs: \( \beta^{(j)} \), \( R^2 \), relative variance

### 4. Forecasting

For each frequency where drought impact is significant:
- Fit ARIMA(p,d,q) vs. ARIMAX(p,d,q) models
- Compare performance with RMSE, MAE, and Success Ratio (SR)

## ✅ Results (Expected)

- Significant drought impact found in frequency bands 2–4 and 32–64 months
- ARIMAX models often outperform ARIMA in longer cycles
- Soybeans, wheat, and maize show the strongest climate-price links

## 🧠 Notes

- Forecasting models are tuned per frequency with AIC selection
- No seasonal adjustment applied due to wavelet filtering
- Analysis assumes covariance-stationary series (via ADF test)

## 📖 References

- Racocha, K. (2020). *Impact of Drought on Commodity Market Forecasting.*
- Bandi, F. et al. (2019). *Spectral Factor Models.* JH Carey Business School.
- Vicente-Serrano et al. (2010). *SPEI Index Methodology.*

---

This work is part of a broader effort to assess how climatic variables affect commodity markets and risk.
