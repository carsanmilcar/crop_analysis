import requests

def download_file(url, output_path):
    """
    Downloads a file from a given URL and saves it to the specified output path.
    """
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Verifica que la petición se realizó correctamente
        
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)
                    
        print(f"File downloaded successfully: {output_path}")
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error occurred: {err}")

if __name__ == "__main__":
    # URL de descarga fija del archivo de precios mensuales del cocoa
    url = r"https://thedocs.worldbank.org/en/doc/18675f1d1639c7a34d463f59263ba0a2-0050012025/related/CMO-Historical-Data-Monthly.xlsx"
    output_file = r"data_inputs/stock_prices/CMO-Historical-Data-Monthly.xlsx"
    download_file(url, output_file)
