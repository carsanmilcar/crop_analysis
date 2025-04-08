import numpy as np
import pandas as pd
import pywt

def haar_decompose(series, wavelet='haar', max_level=6):
    """
    Decomposes a time series using the Haar wavelet transform up to max_level.
    
    Parameters:
        series (pandas.Series): Time series to decompose.
        wavelet (str): Wavelet type. Defaults to 'haar'.
        max_level (int): Maximum decomposition level.
        
    Returns:
        list: Coefficients [approximation, detail1, detail2, ..., detailN]
    """
    coeffs = pywt.wavedec(series, wavelet, level=max_level)
    return coeffs

def run_spectral_decomposition(df_cocoa):
    """
    Performs spectral decomposition on the cocoa dataset.
    
    Parameters:
        df_cocoa (pandas.DataFrame): DataFrame with a DateTime index and one column 
                                     containing cocoa prices.
                                     
    Returns:
        tuple: (cocoa_series, spectral_components)
               cocoa_series: The original cocoa price series (numeric).
               spectral_components: Dictionary with components by scale (e.g., 'j=1', 'j=2', ..., 'approx').
    """
    # Extraer la serie de cocoa (se asume que la DataFrame contiene solo la columna de interés)
    cocoa_series = df_cocoa.iloc[:, 0]
    
    # Convertir la serie a numérica y eliminar NaN
    cocoa_series = pd.to_numeric(cocoa_series, errors='coerce').dropna()
    
    # Realizar la descomposición con el filtro Haar usando un máximo de 6 niveles
    coeffs = haar_decompose(cocoa_series, max_level=6)
    
    # Construir el diccionario de componentes espectrales:
    spectral_components = {}
    # coeffs[0] es la aproximación a nivel máximo
    spectral_components['approx'] = coeffs[0]
    # Los detalles se guardan en coeffs[1:] (se enumeran desde j=1)
    for j, detail in enumerate(coeffs[1:], start=1):
        spectral_components[f'j={j}'] = detail
    
    return cocoa_series, spectral_components

if __name__ == '__main__':
    # Para la demo, simulamos df_cocoa con datos desde enero de 1960.
    time = pd.date_range(start='1960-01-01', periods=780, freq='M')  # 65 años mensuales
    # Simular precios de cocoa (por ejemplo, una media de 2000 USD y desviación 100)
    simulated_prices = np.random.randn(780) * 100 + 2000
    df_cocoa = pd.DataFrame(simulated_prices, index=time, columns=['Cocoa Price'])
    
    cocoa_series, spec_components = run_spectral_decomposition(df_cocoa)
    print("Spectral decomposition complete for df_cocoa:")
    print("Cocoa series length:", len(cocoa_series))
    for key, comp in spec_components.items():
        print(f"{key}: length = {len(comp)}")
