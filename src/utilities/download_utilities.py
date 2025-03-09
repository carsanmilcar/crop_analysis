import os
import re
import zipfile
from typing import Optional
import aiohttp
import asyncio
import requests
from typing import Union, List
import zipfile
import urllib.parse
from typing import List, Union, Optional, Dict

def download_file(url: str, directory: str, filename: Optional[str] = None):
    """Download a file in chunks, with timeout and existence check."""
    import os
    import requests

    # Si se proporciona el nombre, construye la ruta y verifica si ya existe
    if filename is not None:
        filepath = os.path.join(directory, filename)
        if os.path.exists(filepath):
            # Puedes usar logging.info en lugar de print si tienes configurado un logger.
            print(f"File already exists, skipping: {filepath}")
            return filename

    # Realiza la solicitud con timeout
    with requests.get(url, stream=True, timeout=60) as r:
        # Si no se proporcionó un nombre, intenta obtenerlo de los headers
        if filename is None:
            filename = get_filename_from_cd(r.headers.get("content-disposition", ""))
            if not filename:
                raise ValueError("filename not provided and cannot infer from content-disposition")
        # Construir la ruta completa del archivo
        filepath = os.path.join(directory, filename)
        # Verificar nuevamente por si el archivo ya se descargó en otra ocasión
        if os.path.exists(filepath):
            print(f"File already exists, skipping: {filepath}")
            return filename

        r.raise_for_status()
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return filename


def get_filename_from_cd(content_disp):
    """Get filename from content-disposition."""
    if not content_disp:
        return None
    filename = re.findall("filename=(.+)", content_disp)
    if len(filename) == 0:
        return None
    return filename[0]
def download_and_extract_archive(
    url: str,
    output_dir: str,
    files_to_extract: Union[str, List[str]] = None,
    extract_by_name: str = None,
    extract_by_extension: str = None,
    overwrite: bool = False,
):
    """
    Descarga un archivo comprimido desde la URL especificada o, si ya existe localmente,
    lo utiliza para extraer los archivos especificados.
    """
    os.makedirs(output_dir, exist_ok=True)

    # Si 'url' es una ruta local existente, la usamos directamente.
    if os.path.exists(url):
        archive_path = url
    else:
        # Obtener el nombre del archivo desde la URL
        parsed_url = urllib.parse.urlparse(url)
        filename = os.path.basename(parsed_url.path)
        archive_path = os.path.join(output_dir, filename)
        if not overwrite and os.path.exists(archive_path):
            pass  # El archivo ya existe, se asume que es correcto
        else:
            # Aquí se debe implementar o llamar a la función de descarga
            download_file(url, output_dir, filename)

    # Abrir el archivo ZIP y extraer los archivos que cumplan el criterio
    with zipfile.ZipFile(archive_path, "r") as zip_ref:
        available_files = zip_ref.namelist()

        if extract_by_name:
            files_to_extract = [
                file for file in available_files if os.path.splitext(file)[0] == extract_by_name
            ]
        elif extract_by_extension:
            files_to_extract = [
                file for file in available_files if file.endswith(extract_by_extension)
            ]

        # Asegurarse de que files_to_extract sea una lista
        if isinstance(files_to_extract, str):
            files_to_extract = [files_to_extract]

        if not files_to_extract:
            raise ValueError("No se encontraron archivos que coincidan con el criterio de extracción.")

        for file in files_to_extract:
            zip_ref.extract(file, output_dir)

    # Eliminar el archivo ZIP después de la extracción
    try:
        os.remove(archive_path)
    except OSError as e:
        raise RuntimeError(f"Error al eliminar el archivo '{archive_path}': {e}")

def get_filename_from_cd(content_disp):
    """Get filename from content-disposition."""
    if not content_disp:
        return None
    filename = re.findall("filename=(.+)", content_disp)
    if len(filename) == 0:
        return None
    return filename[0]
async def download_file_async(url: str, directory: str, filename: Optional[str] = None, 
                                session: Optional[aiohttp.ClientSession] = None, max_attempts: int = 5) -> Optional[str]:
    """
    Asynchronously download a file in chunks using aiohttp with persistent session support.
    
    Args:
        url (str): URL of the file to download.
        directory (str): Directory where the file will be saved.
        filename (Optional[str], optional): The target filename. If None, a HEAD request is used to infer it.
        session (Optional[aiohttp.ClientSession], optional): A persistent aiohttp session.
        max_attempts (int, optional): Maximum number of download attempts. Defaults to 5.
    
    Returns:
        Optional[str]: The filename if the download was successful, or None otherwise.
    """
    # If no persistent session is provided, create one and use it locally.
    if session is None:
        async with aiohttp.ClientSession() as new_session:
            return await download_file_async(url, directory, filename, session=new_session, max_attempts=max_attempts)

    if filename is None:
        async with session.head(url) as head_response:
            head_response.raise_for_status()
            content_disp = head_response.headers.get("content-disposition", "")
            filename = get_filename_from_cd(content_disp)
            if not filename:
                raise ValueError("filename not provided and cannot infer from content-disposition")
    filepath = os.path.join(directory, filename)
    if os.path.exists(filepath):
        return filename

    for attempt in range(1, max_attempts + 1):
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(filepath, "wb") as f:
                    while True:
                        chunk = await response.content.read(8192)
                        if not chunk:
                            break
                        f.write(chunk)
                return filename
        except Exception as exc:
            wait_time = 0.32 * attempt  # Simple backoff strategy.
            await asyncio.sleep(wait_time)
    return None