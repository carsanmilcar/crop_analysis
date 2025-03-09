import os
import calendar
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Any

import ee
import geopandas as gpd
from google.oauth2 import service_account
from shapely.geometry import Polygon, MultiPolygon

import rasterio
from rasterio.merge import merge

from dataset_generation.utilities.download_utilities import download_file, get_filename_from_cd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def merge_rasters(file_list: List[str], output_path: str) -> None:
    """
    Merge multiple raster files into a single GeoTIFF using rasterio.merge.merge.

    Args:
        file_list (List[str]): List of file paths to raster files.
        output_path (str): Path where the merged file will be saved.
    """
    src_files = []
    try:
        for fp in file_list:
            src = rasterio.open(fp)
            src_files.append(src)
        mosaic, out_trans = merge(src_files)
        out_meta = src_files[0].meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans,
        })
        with rasterio.open(output_path, "w", **out_meta) as dest:
            dest.write(mosaic)
        logger.info(f"Merged raster saved at: {output_path}")
    except Exception as exc:
        logger.error(f"Error during merging rasters: {exc}")
    finally:
        for src in src_files:
            src.close()


def initialize_gee(credentials_path: str) -> None:
    """
    Initialize Google Earth Engine with the provided credentials.

    Args:
        credentials_path (str): Path to the service account credentials file.
    """
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=[
                "https://www.googleapis.com/auth/earthengine",
                "https://www.googleapis.com/auth/cloud-platform",
            ],
        )
        ee.Initialize(credentials)
        logger.info("Google Earth Engine initialized successfully.")
    except Exception as exc:
        logger.error(f"Failed to initialize GEE: {exc}")
        raise


def download_gee_data_single_collection(
    gdf: gpd.GeoDataFrame,
    column_name: str,
    collection: str,
    band: str,
    years: Optional[List[int]],
    output_folder: str,
    scale: int = 500,
    crs: str = "EPSG:4326",
    max_workers: int = 5,
    frequency: str = "yearly",
) -> None:
    """
    Downloads GEE data for a single collection and band, handling both yearly and monthly frequencies.
    If a region's geometry is a multipolygon, it splits it into individual polygons, downloads each one,
    and then merges the resulting files into a final GeoTIFF.

    Args:
        gdf (GeoDataFrame): GeoDataFrame containing region geometries.
        column_name (str): Column in `gdf` that holds unique region identifiers.
        collection (str): Earth Engine ImageCollection identifier.
        band (str): Band name to select from the collection.
        years (List[int], optional): List of years to download. If None, downloads without date filtering.
        output_folder (str): Directory to save the downloaded GeoTIFF files.
        scale (int, optional): Scale in meters for the data. Defaults to 500.
        crs (str, optional): Coordinate reference system for reprojection. Defaults to "EPSG:4326".
        max_workers (int, optional): Maximum number of parallel downloads. Defaults to 5.
        frequency (str, optional): Either "yearly" or "monthly". Defaults to "yearly".
    """
    if frequency not in ("yearly", "monthly", "monthly_mosaic"):
        raise ValueError(f"Unsupported frequency: {frequency}")

    os.makedirs(output_folder, exist_ok=True)

    def process_region(region_code: str, region_geometry: Any, year: Optional[int] = None) -> None:
        """
        Processes a single region (and optionally a specific year) to download and save the data.
        If the region geometry is a multipolygon, it downloads each polygon separately and then merges them.

        Args:
            region_code (str): Unique identifier for the region.
            region_geometry: Geometry of the region (can be Polygon or MultiPolygon).
            year (int, optional): Year for which to download the data.
        """
        try:
            # Prepare the base ImageCollection and filter by year if needed.
            image_collection = ee.ImageCollection(collection).select(band)
            if year is not None:
                start_date = f"{year}-01-01"
                end_date = f"{year}-12-31"
                image_collection = image_collection.filterDate(start_date, end_date)

            region_folder = os.path.join(output_folder, region_code)
            os.makedirs(region_folder, exist_ok=True)

            def get_bbox_from_polygon(poly: Polygon) -> List:
                minx, miny, maxx, maxy = poly.bounds
                return [
                    [minx, miny],
                    [maxx, miny],
                    [maxx, maxy],
                    [minx, maxy],
                ]

            def download_for_bbox(bbox: List, filename: str, img: ee.Image) -> Optional[str]:
                """
                Download the image for a given bounding box, retrying if necessary.
                
                Args:
                    bbox (List): Bounding box coordinates.
                    filename (str): The target filename.
                    img (ee.Image): Earth Engine image.
                
                Returns:
                    Optional[str]: The file path if downloaded successfully, or None.
                """
                filepath = os.path.join(region_folder, filename)
                if os.path.exists(filepath):
                    logger.info(f"File already exists. Skipping: {filepath}")
                    return filepath

                params = {"scale": scale, "region": bbox, "format": "GeoTIFF", "crs": crs}
                max_attempts = 5
                # Try up to max_attempts times with a small increasing delay between attempts.
                for attempt in range(1, max_attempts + 1):
                    try:
                        download_url = img.getDownloadURL(params)
                        download_file(download_url, region_folder, filename)
                        logger.info(f"Downloaded file: {filepath}")
                        return filepath
                    except Exception as exc:
                        wait_time = 0.32 * attempt  # a simple backoff strategy
                        logger.warning(
                            f"Error downloading {filename} (attempt {attempt} of {max_attempts}): {exc}. "
                            f"Sleeping {wait_time} seconds before retry."
                        )
                        time.sleep(wait_time)
                logger.error(f"Failed to download {filename} after {max_attempts} attempts.")
                return None

            def process_image(img: ee.Image, time_suffix: str) -> None:
                """
                Handles downloading for either a single polygon or a multipolygon.
                For multipolygons, downloads each part and then merges them.
                """
                if region_geometry.geom_type == "MultiPolygon":
                    temp_files = []
                    for idx, poly in enumerate(region_geometry.geoms):
                        bbox = get_bbox_from_polygon(poly)
                        part_filename = f"{collection.split('/')[-1]}_{band}_{time_suffix}_{region_code}_part{idx}.tif"
                        temp_filepath = download_for_bbox(bbox, part_filename, img)
                        if temp_filepath:
                            temp_files.append(temp_filepath)
                    if not temp_files:
                        logger.warning(f"No valid downloads for multipolygon region {region_code}, {time_suffix}")
                        return
                    # Merge the downloaded parts.
                    merged_filename = f"{collection.split('/')[-1]}_{band}_{time_suffix}_{region_code}.tif"
                    merged_filepath = os.path.join(region_folder, merged_filename)
                    merge_rasters(temp_files, merged_filepath)
                    # Optionally remove temporary files.
                    for fp in temp_files:
                        try:
                            os.remove(fp)
                            logger.info(f"Removed temporary file: {fp}")
                        except Exception as e:
                            logger.warning(f"Could not remove temp file {fp}: {e}")
                else:
                    # Single polygon case.
                    bbox = get_bbox_from_polygon(region_geometry)
                    final_filename = f"{collection.split('/')[-1]}_{band}_{time_suffix}_{region_code}.tif"
                    download_for_bbox(bbox, final_filename, img)

            # Process based on the frequency.
            if frequency == "yearly":
                img = image_collection.mosaic()
                img = img.reproject(crs=crs, scale=scale)
                time_suffix = f"{year}" if year is not None else ""
                process_image(img, time_suffix)
            elif frequency == "monthly":
                if year is None:
                    logger.warning("Year is required for monthly frequency. Skipping region: %s", region_code)
                    return
                for month in range(1, 13):
                    start_date = f"{year}-{month:02d}-01"
                    end_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
                    monthly_collection = image_collection.filterDate(start_date, end_date)
                    img = monthly_collection.mosaic()
                    img = img.reproject(crs=crs, scale=scale)
                    time_suffix = f"{year}_{month:02d}"
                    process_image(img, time_suffix)
            elif frequency == "monthly_mosaic":
                if year is None:
                    logger.warning("Year is required for monthly_mosaic frequency. Skipping region: %s", region_code)
                    return
                monthly_images = []
                for month in range(1, 13):
                    start_date = f"{year}-{month:02d}-01"
                    end_date = f"{year}-{month:02d}-{calendar.monthrange(year, month)[1]}"
                    monthly_collection = image_collection.filterDate(start_date, end_date)
                    monthly_img = monthly_collection.mosaic()
                    monthly_img = monthly_img.reproject(crs=crs, scale=scale)
                    monthly_images.append(monthly_img)
                # Combine all monthly images into one annual mosaic
                mosaic_img = ee.ImageCollection(monthly_images).mosaic()
                time_suffix = f"{year}_monthly_mosaic"
                process_image(mosaic_img, time_suffix)

        except Exception as exc:
            logger.error(f"Unexpected error for region {region_code}, year {year}: {exc}")

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, row in gdf.iterrows():
            region_code = row[column_name]
            region_geometry = row.geometry
            if years:
                for yr in years:
                    tasks.append(executor.submit(process_region, region_code, region_geometry, yr))
            else:
                tasks.append(executor.submit(process_region, region_code, region_geometry))
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Error in thread execution: {exc}")

    logger.info("Processing complete.")

def download_gee_data_single_image(
    gdf,
    column_name,
    image_id,
    band,
    output_folder,
    scale=500,
    crs="EPSG:4326",
    max_workers=5,
):
    """
    Downloads a single GEE Image for specified regions without clipping.

    Args:
        gdf (GeoDataFrame): GeoDataFrame containing region geometries.
        column_name (str): Column name in `gdf` containing unique region identifiers.
        image_id (str): Earth Engine Image ID to download.
        band (str): Band name to select from the Image.
        output_folder (str): Folder to save the downloaded GeoTIFFs.
        scale (int, optional): Scale in meters for the data. Defaults to 500.
        crs (str, optional): Coordinate reference system for reprojection. Defaults to "EPSG:4326".
        max_workers (int, optional): Maximum number of parallel downloads. Defaults to 5.
    """
    os.makedirs(output_folder, exist_ok=True)

    
    def process_region(region_code, region_geometry):
        """
        Processes a single region.

        Args:
            region_code (str): Unique identifier for the region.
            region_geometry (shapely.geometry): Geometry of the region.
        """
        filename = f"{image_id.split('/')[-1]}_{band}_{region_code}.tif"
        region_folder = os.path.join(output_folder, region_code)
        os.makedirs(region_folder, exist_ok=True)
        filepath = os.path.join(region_folder, filename)

        # Check if the file already exists
        if os.path.exists(filepath):
            print(f"File already exists. Skipping: {filepath}")
            return

        # Retrieve the Image from Earth Engine
        try:
            image = ee.Image(image_id).select(band).reproject(crs=crs, scale=scale)
        except Exception as e:
            print(f"Error retrieving image {image_id} for region {region_code}: {e}")
            return

        try:
            # Use the bounding box for downloading
            bounds = region_geometry.bounds
            region_bbox = [
                [bounds[0], bounds[1]],
                [bounds[2], bounds[1]],
                [bounds[2], bounds[3]],
                [bounds[0], bounds[3]],
            ]

            # Generate the download URL
            download_url = image.getDownloadURL(
                {
                    "scale": scale,
                    "region": region_bbox,
                    "format": "GeoTIFF",
                    "crs": crs,
                }
            )

            # Download the file
            download_file(download_url,region_folder, filename)

        except Exception as e:
            print(f"Error processing region {region_code}: {e}")

    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for _, row in gdf.iterrows():
            region_code = row[column_name]
            region_geometry = row.geometry
            tasks.append(executor.submit(process_region, region_code, region_geometry))

        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {e}")

    print("Processing complete.")


def merge_tifs_in_directory(
    input_dir,
    output_filepath,
    filter_gdf=None,
    filter_crs=None,
    target_epsg=None,
    delete_after_merge=False,
    nodata=None,
):
    """
    Merges all .tif files inside a directory (including subdirectories) into a single GeoTIFF file,
    with options to filter, reproject, and delete source files.

    Args:
        input_dir (str): Path to the directory containing .tif files.
        output_filepath (str): Path to save the merged GeoTIFF file.
        filter_gdf (GeoDataFrame, optional): GeoDataFrame to filter .tif files by geometry intersection. Defaults to None.
        filter_crs (str, optional): CRS of the filter GeoDataFrame. Defaults to None.
        target_epsg (int, optional): EPSG code to reproject the merged GeoTIFF. Defaults to None.
        delete_after_merge (bool, optional): If True, deletes source .tif files after merging. Defaults to False.
        nodata (int/float, optional): Value to use as nodata in the merged file. Defaults to None.

    Returns:
        None
    """
    # Search recursively for all .tif files in the directory
    print(f"Searching for .tif files in {input_dir}...")
    tif_files = glob.glob(os.path.join(input_dir, "**", "*.tif"), recursive=True)

    if not tif_files:
        print("No .tif files found in the specified directory.")
        return

    print(f"Found {len(tif_files)} .tif files.")

    # If filtering with a GeoDataFrame is enabled
    if filter_gdf is not None:
        if filter_crs:
            filter_gdf = filter_gdf.to_crs(filter_crs)
        filtered_files = []
        for tif_file in tif_files:
            try:
                with rasterio.open(tif_file) as src:
                    bounds = box(*src.bounds)
                    bounds_gdf = gpd.GeoDataFrame({"geometry": [bounds]}, crs=src.crs)
                    if (
                        bounds_gdf.to_crs(filter_gdf.crs)
                        .geometry[0]
                        .intersects(filter_gdf.unary_union)
                    ):
                        filtered_files.append(tif_file)
            except Exception as e:
                print(f"Error checking intersection for {tif_file}: {e}")
        tif_files = filtered_files
        print(f"Filtered to {len(tif_files)} .tif files after GeoDataFrame filtering.")

    if not tif_files:
        print("No .tif files remain after filtering.")
        return

    # Open all .tif files using rasterio
    datasets = []
    for tif_file in tif_files:
        try:
            src = rasterio.open(tif_file)
            datasets.append(src)
        except Exception as e:
            print(f"Error opening file {tif_file}: {e}")

    if not datasets:
        print("No valid .tif files were opened. Aborting merge.")
        return

    # Perform the merge
    print("Merging datasets...")
    try:
        merged_data, merged_transform = merge(datasets, nodata=nodata)
    except Exception as e:
        print(f"Error during merging: {e}")
        return
    finally:
        # Close all datasets
        for dataset in datasets:
            dataset.close()

    # Use metadata from the first dataset as a base for the output
    out_meta = datasets[0].meta.copy()
    out_meta.update(
        {
            "driver": "GTiff",
            "height": merged_data.shape[1],
            "width": merged_data.shape[2],
            "transform": merged_transform,
            "nodata": nodata,
        }
    )

    # If target EPSG is specified, reproject the merged dataset
    if target_epsg:
        print(f"Reprojecting merged GeoTIFF to EPSG:{target_epsg}...")
        dst_transform, dst_width, dst_height = calculate_default_transform(
            out_meta["crs"],
            f"EPSG:{target_epsg}",
            out_meta["width"],
            out_meta["height"],
            *out_meta["bounds"],
        )
        out_meta.update(
            {
                "crs": f"EPSG:{target_epsg}",
                "transform": dst_transform,
                "width": dst_width,
                "height": dst_height,
            }
        )
        reprojected_data = rasterio.band(merged_data, 1)
        with rasterio.open(output_filepath, "w", **out_meta) as dest:
            reproject(
                source=merged_data,
                destination=reprojected_data,
                src_transform=merged_transform,
                src_crs=out_meta["crs"],
                dst_transform=dst_transform,
                dst_crs=f"EPSG:{target_epsg}",
                resampling=Resampling.nearest,
            )
            print(f"Reprojected GeoTIFF saved successfully to {output_filepath}.")
    else:
        # Save the merged GeoTIFF
        print(f"Saving merged GeoTIFF to {output_filepath}...")
        try:
            with rasterio.open(output_filepath, "w", **out_meta) as dest:
                dest.write(merged_data)
            print(f"Merged GeoTIFF saved successfully: {output_filepath}")
        except Exception as e:
            print(f"Error saving merged GeoTIFF: {e}")

    # Optionally delete the original .tif files
    if delete_after_merge:
        print("Deleting source .tif files...")
        for tif_file in tif_files:
            try:
                os.remove(tif_file)
                print(f"Deleted {tif_file}")
            except Exception as e:
                print(f"Error deleting file {tif_file}: {e}")
