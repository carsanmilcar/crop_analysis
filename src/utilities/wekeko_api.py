import requests
import time
import logging

logging.basicConfig(level=logging.INFO)


class WekeoHDA:
    """
    A class to interact with the WEKEO Harmonized Data Access (HDA) API.

    The workflow is as follows:
    1. Submit a download request with desired parameters.
    2. Poll the API until the request is processed.
    3. Once ready, retrieve the download URL and download the data.

    Attributes:
        api_key (str): API key for authentication.
        base_url (str): Base URL for the HDA API.
    """

    def __init__(self, api_key, base_url="https://hda-api.wekeo.eu/api"):
        """
        Initialize the WekeoHDA client.

        Args:
            api_key (str): Your API key for the HDA API.
            base_url (str, optional): Base API URL. Defaults to "https://hda-api.wekeo.eu/api".
        """
        self.api_key = api_key
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def submit_download_request(self, payload):
        """
        Submit a download request to the HDA API.

        Args:
            payload (dict): A dictionary of parameters as specified in the HDA API documentation.

        Returns:
            str: The request ID assigned to the job.

        Raises:
            HTTPError: If the POST request fails.
        """
        endpoint = f"{self.base_url}/requests"
        logging.info("Submitting download request to %s", endpoint)
        response = requests.post(endpoint, json=payload, headers=self.headers)
        response.raise_for_status()
        request_id = response.json().get("request_id")
        logging.info("Download request submitted. Request ID: %s", request_id)
        return request_id

    def poll_request_status(self, request_id, poll_interval=10, timeout=300):
        """
        Poll the status of the submitted request until it is finished.

        Args:
            request_id (str): The ID of the submitted request.
            poll_interval (int, optional): Seconds to wait between polls. Defaults to 10.
            timeout (int, optional): Maximum time in seconds to wait. Defaults to 300.

        Returns:
            dict: The JSON response of the finished request including the download URL.

        Raises:
            Exception: If the request fails or times out.
        """
        endpoint = f"{self.base_url}/requests/{request_id}"
        logging.info("Polling request status at %s", endpoint)
        start_time = time.time()
        while True:
            response = requests.get(endpoint, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            status = data.get("status")
            logging.info("Current status: %s", status)
            if status == "finished":
                logging.info("Request finished successfully.")
                return data
            elif status == "error":
                raise Exception("Download request failed: %s" % data.get("error_message"))
            if time.time() - start_time > timeout:
                raise TimeoutError("Polling timed out after %s seconds." % timeout)
            time.sleep(poll_interval)

    def download_file(self, download_url, output_path):
        """
        Download the file from the provided download URL and save it to output_path.

        Args:
            download_url (str): The URL from which to download the data.
            output_path (str): Local path to save the downloaded file.

        Raises:
            HTTPError: If the GET request fails.
        """
        logging.info("Downloading file from %s", download_url)
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        with open(output_path, "wb") as file_handle:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file_handle.write(chunk)
        logging.info("File downloaded successfully and saved to %s", output_path)

    def download_data(self, payload, output_path, poll_interval=10, timeout=300):
        """
        Complete workflow to submit a download request, poll for completion,
        and download the resulting file.

        Args:
            payload (dict): Parameters for the download request.
            output_path (str): Local file path where the downloaded data will be saved.
            poll_interval (int, optional): Seconds between polling attempts. Defaults to 10.
            timeout (int, optional): Maximum seconds to wait for the request to finish. Defaults to 300.

        Raises:
            Exception: If no download URL is returned.
        """
        request_id = self.submit_download_request(payload)
        result_data = self.poll_request_status(request_id, poll_interval, timeout)
        download_url = result_data.get("download_url")
        if not download_url:
            raise Exception("No download URL provided in the response.")
        self.download_file(download_url, output_path)


if __name__ == "__main__":
    # Example usage:
    # Define your API key and payload according to HDA API specifications.
    API_KEY = "YOUR_API_KEY"
    payload = {
        "dataset": "example_dataset",
        "time_range": {"start": "2021-01-01", "end": "2021-01-31"},
        "area": {"bbox": [12.0, 42.0, 13.0, 43.0]},
        "format": "GeoTIFF",
    }
    output_file = "downloaded_data.tif"

    # Initialize the client and download the data.
    hda_client = WekeoHDA(API_KEY)
    try:
        hda_client.download_data(payload, output_file)
    except Exception as exc:
        logging.error("An error occurred: %s", exc)