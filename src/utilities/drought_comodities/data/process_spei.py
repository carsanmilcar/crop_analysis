import requests

def get_final_url(initial_url):
    """
    Makes a request to the initial URL and returns the final URL after following any redirects.
    """
    response = requests.get(initial_url, allow_redirects=True)
    response.raise_for_status()
    return response.url

def download_file(final_url, output_path):
    """
    Downloads the file from the final URL and saves it to the specified path.
    """
    response = requests.get(final_url, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
    print("File downloaded successfully:", output_path)

if __name__ == "__main__":
    # URL provided on the SPEI page
    initial_url = "https://spei.csic.es/spei_database/nc/spei04.nc"
    # First, obtain the final URL after redirects
    final_url = get_final_url(initial_url)
    print("Final URL obtained:", final_url)
    
    # Using the obtained final URL, proceed to download the file
    output_file = r"data_inputs/spei_downloads/spei04.nc"
    download_file(final_url, output_file)
