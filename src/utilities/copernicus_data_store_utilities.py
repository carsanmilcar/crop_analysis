import os
import cdsapi
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
from src.utilities.download_utilities import download_and_extract_archive
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import logging
import time

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suprimir logging de cdsapi
logging.getLogger("cdsapi").addHandler(logging.NullHandler())
logging.getLogger("cdsapi").propagate = False
logging.getLogger("cdsapi").setLevel(logging.CRITICAL)

# Cargar variables de entorno
load_dotenv()

# Credenciales de CDS API
cdsapi_url = os.getenv("CDSAPI_URL")
cdsapi_key = os.getenv("CDSAPI_KEY")

if not cdsapi_url or not cdsapi_key:
    raise ValueError(
        "CDS API URL or Key not found in environment variables. Check your .env file."
    )

# Escribir configuración en .cdsapirc
cdsapirc_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapirc_path, "w") as f:
    f.write(f"url: {cdsapi_url}\nkey: {cdsapi_key}\n")

# Crear cliente CDS API
client = cdsapi.Client()

def download_cds_dataset(
    dataset,
    request,
    output_dir: Path,
    files_to_extract: list = None,
    extract_by_name: str = None,
    extract_by_extension: str = None,
    overwrite: bool = False,
    max_retries: int = 3,
    retry_delay: int = 5,
):
    """
    Descarga y extrae datos de CDS con manejo de reintentos.
    """
    try:
        # Construir nombre único para el archivo
        archive_name = (
            f"{dataset}_{'_'.join(request['period'])}_{request['experiment']}_{request['product_type']}_{request['variable']}"
        )
        # Descargar como archivo zip (no .nc)
        archive_path = output_dir / f"{archive_name}.zip"

        # Verificar si el archivo ya existe
        if not overwrite and archive_path.exists():
            logger.info(f"Archivo ya existe: {archive_path}. Skipping download.")
        else:
            # Intentar descargar con reintentos
            for attempt in range(1, max_retries + 1):
                try:
                    logger.info(f"Descargando {archive_name} (Intento {attempt})...")
                    client.retrieve(dataset, request).download(str(archive_path))
                    logger.info(f"Descarga completada: {archive_path}")
                    break  # Salir del loop si la descarga es exitosa
                except Exception as e:
                    logger.warning(f"Error en descarga de {archive_name}: {e}")
                    if attempt < max_retries:
                        logger.info(f"Reintentando en {retry_delay} segundos...")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Fallo en descarga de {archive_name} después de {max_retries} intentos.")
                        raise
        # ...
        archive_path = output_dir / f"{archive_name}.zip"
        # Luego, al llamar a la función de extracción:
        if extract_by_extension:
            download_and_extract_archive(
                url=str(archive_path),  # Convertir a cadena para evitar problemas
                files_to_extract=files_to_extract,
                extract_by_name=extract_by_name,
                output_dir=str(output_dir),
                extract_by_extension=extract_by_extension,
                overwrite=overwrite,
            )
    except Exception as e:
        logger.error(f"Error al descargar o procesar {archive_name}: {e}")
        raise
def parallel_download_cds(
    download_tasks: list, max_workers: int = 2, output_dir: Path = Path("data_inputs")
):
    """
    Descarga múltiples datasets de CDS en paralelo con manejo de reintentos.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                download_cds_dataset,
                task["dataset"],
                task["request"],
                output_dir,
                task.get("files_to_extract"),
                task.get("extract_by_name"),
                task.get("extract_by_extension"),
                task.get("overwrite", False),
            ): task
            for task in download_tasks
        }

        with tqdm(total=len(futures), desc="Downloading datasets") as pbar:
            for future in as_completed(futures):
                task = futures[future]
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Fallo en tarea de descarga: {e}")
                pbar.update(1)

def create_general_download_tasks(
    base_request: Dict[str, Any],
    scenarios: Dict[str, List[str]],
    product_types: List[str],
    dataset: str,
    output_dir: Path,
    variable: List[str],
    extract_by_extension: Optional[str] = None,
    overwrite: bool = False,
) -> List[Dict[str, Any]]:
    """
    Crea una lista de tareas de descarga generalizadas para descargas paralelas.
    """
    download_tasks = []
    for scenario, periods in scenarios.items():
        for period in periods:
            for product_type in product_types:
                # Construir solicitud extendiendo la base_request
                request = base_request.copy()
                request.update(
                    {
                        "product_type": product_type,
                        "experiment": scenario,
                        "period": [period],
                        "variable": variable,
                    }
                )

                download_tasks.append(
                    {
                        "dataset": dataset,
                        "request": request,
                        "files_to_extract": None,
                        "extract_by_name": None,
                        "extract_by_extension": extract_by_extension,
                        "overwrite": overwrite,
                    }
                )
    return download_tasks
