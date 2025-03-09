import ee
import os
import requests
import geopandas as gpd
from shapely.geometry import mapping
from google.oauth2 import service_account
from concurrent.futures import ThreadPoolExecutor, as_completed
from download_utilities import download_file
import rasterio
from rasterio.merge import merge
import rasterio.mask
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s',
    handlers=[
        logging.FileHandler("gee_downloader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def initialize_gee(credentials_path):
    """
    Initializes Google Earth Engine with the provided credentials.

    Args:
        credentials_path (str): Path to the service account JSON credentials file.
    """
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/earthengine"]
    )
    ee.Initialize(credentials)
    logger.info("Google Earth Engine initialized.")

def download_gee_data_by_gdf(
    gdf,
    column_name,
    collection,
    band,
    output_folder,
    start_date=None,
    end_date=None,
    scale=500,
    max_workers=5
):
    """
    Downloads data from GEE for geometries defined in a GeoDataFrame with a specific CRS.

    - If the geometry is a MultiPolygon, it is exploded into individual polygons.
    - Downloads each polygon separately.
    - Merges the resulting TIFFs.
    - Masks the merged TIFF to remove data outside the original geometry.
    - Finally, deletes intermediate files.

    Args:
        gdf (GeoDataFrame): GeoDataFrame containing geometries.
        column_name (str): Name of the column with unique identifiers.
        collection (str): GEE image collection ID (e.g., "MODIS/061/MCD12Q1").
        band (str): Specific band to download.
        output_folder (str): Directory where files will be saved.
        start_date (str, optional): Start date in "YYYY-MM-DD" format. If None, no start date filter.
        end_date (str, optional): End date in "YYYY-MM-DD" format. If None, no end date filter.
        scale (int): Spatial resolution in meters.
        max_workers (int): Maximum number of concurrent download threads.
    """
    os.makedirs(output_folder, exist_ok=True)
    logger.info(f"Output folder set to: {output_folder}")

    def process_polygon(region_code, polygon, index):
        """
        Downloads the image corresponding to a specific polygon.

        Args:
            region_code (str): Unique identifier for the region.
            polygon (shapely.geometry.Polygon): Polygon geometry.
            index (int): Index of the polygon within the region.

        Returns:
            str or None: Path to the downloaded TIFF file or None if failed.
        """
        region_folder = os.path.join(output_folder, region_code)
        os.makedirs(region_folder, exist_ok=True)

        # Construct filename based on available dates
        if start_date and end_date:
            filename_date = f"{start_date}_{end_date}"
        elif start_date:
            filename_date = f"{start_date}_NOEND"
        else:
            filename_date = "NO_DATE"

        filename = f"{collection.split('/')[-1]}_{band}_{filename_date}_{region_code}_poly{index}.tif"
        filepath = os.path.join(region_folder, filename)

        if os.path.exists(filepath):
            logger.info(f"File already exists, skipping: {filepath}")
            return filepath

        try:
            # Build the ImageCollection
            image_collection = ee.ImageCollection(collection).select(band)
            
            # Apply date filtering if applicable
            if start_date and end_date:
                image_collection = image_collection.filterDate(start_date, end_date)
            elif start_date:
                image_collection = image_collection.filterDate(start_date, "2099-12-31")
            # If no dates provided, do not filter

            image = image_collection.first()
            if image is None:
                logger.warning(f"No image found for {region_code} in the specified range.")
                return None

            # Create a bounding box for the polygon
            bounds = polygon.bounds
            region_bbox = [
                [bounds[0], bounds[1]],
                [bounds[2], bounds[1]],
                [bounds[2], bounds[3]],
                [bounds[0], bounds[3]],
            ]

            # Download using the GeoDataFrame's CRS
            download_url = image.getDownloadURL({
                "scale": scale,
                "region": region_bbox,
                "format": "GeoTIFF",
                "crs": gdf.crs.to_string()
            })

            download_file(url=download_url, directory=region_folder, filename=filename)
            return filepath

        except Exception as e:
            logger.error(f"Error processing polygon {index} of {region_code}: {e}")
            return None

    def merge_rasters(raster_paths, output_path):
        """
        Merges multiple raster files into a single raster.

        Args:
            raster_paths (list of str): Paths to raster files to merge.
            output_path (str): Path to save the merged raster.

        Returns:
            str or None: Path to the merged raster or None if failed.
        """
        valid_rasters = [rp for rp in raster_paths if rp and os.path.exists(rp)]
        if not valid_rasters:
            logger.warning("No valid rasters to merge.")
            return None

        try:
            src_files_to_mosaic = [rasterio.open(rp) for rp in valid_rasters]
            mosaic, out_trans = merge(src_files_to_mosaic)

            # Take metadata from the first raster
            out_meta = src_files_to_mosaic[0].meta.copy()
            out_meta.update({
                "driver": "GTiff",
                "height": mosaic.shape[1],
                "width": mosaic.shape[2],
                "transform": out_trans
            })

            with rasterio.open(output_path, "w", **out_meta) as dest:
                dest.write(mosaic)

            for src in src_files_to_mosaic:
                src.close()

            logger.info(f"Merged raster saved at: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Error merging rasters: {e}")
            return None

    def mask_raster_with_shapefile(raster_path, shape_geometry, output_path_masked):
        """
        Masks/crops the raster to remove data outside the given geometry.

        Args:
            raster_path (str): Path to the input raster.
            shape_geometry (shapely.geometry.Geometry): Geometry to mask with.
            output_path_masked (str): Path to save the masked raster.

        Returns:
            str or None: Path to the masked raster or None if failed.
        """
        if not os.path.exists(raster_path):
            logger.error(f"Raster does not exist: {raster_path}")
            return None

        shapes = [mapping(shape_geometry)]

        try:
            with rasterio.open(raster_path) as src:
                out_image, out_transform = rasterio.mask.mask(
                    src, 
                    shapes, 
                    crop=True,
                    filled=True,
                    invert=False
                )
                out_meta = src.meta.copy()
                out_meta.update({
                    "driver": "GTiff",
                    "height": out_image.shape[1],
                    "width": out_image.shape[2],
                    "transform": out_transform
                })

            with rasterio.open(output_path_masked, "w", **out_meta) as dest:
                dest.write(out_image)

            logger.info(f"Masked raster saved at: {output_path_masked}")
            return output_path_masked

        except Exception as e:
            logger.error(f"Error masking raster {raster_path}: {e}")
            return None

    def process_region(region_code, region_geometry):
        """
        Processes a single region:
        1. Explodes geometry if it's a MultiPolygon.
        2. Downloads each polygon separately.
        3. Merges all resulting TIFFs.
        4. Masks the final TIFF.
        5. Deletes intermediate TIFF files.

        Args:
            region_code (str): Unique identifier for the region.
            region_geometry (shapely.geometry.Geometry): Geometry of the region.
        """
        if region_geometry.geom_type == "MultiPolygon":
            exploded_polygons = list(region_geometry.geoms)
            logger.debug(f"Region {region_code} is a MultiPolygon with {len(exploded_polygons)} polygons.")
        elif region_geometry.geom_type == "Polygon":
            exploded_polygons = [region_geometry]
            logger.debug(f"Region {region_code} is a Polygon.")
        else:
            exploded_polygons = [region_geometry]
            logger.debug(f"Region {region_code} has an unsupported geometry type: {region_geometry.geom_type}")

        downloaded_rasters = []
        for i, poly in enumerate(exploded_polygons):
            raster_path = process_polygon(region_code, poly, i)
            if raster_path:
                downloaded_rasters.append(raster_path)

        if not downloaded_rasters:
            logger.warning(f"No rasters downloaded for region {region_code}.")
            return

        # Construct filenames based on available dates
        if start_date and end_date:
            date_str = f"{start_date}_{end_date}"
        elif start_date:
            date_str = f"{start_date}_NOEND"
        else:
            date_str = "NO_DATE"

        merged_filename = f"{collection.split('/')[-1]}_{band}_{date_str}_{region_code}_merged.tif"
        merged_raster_path = os.path.join(output_folder, region_code, merged_filename)

        merged_raster_path = merge_rasters(downloaded_rasters, merged_raster_path)
        if not merged_raster_path:
            return

        masked_filename = f"{collection.split('/')[-1]}_{band}_{date_str}_{region_code}_masked.tif"
        masked_raster_path = os.path.join(output_folder, region_code, masked_filename)
        final_mask = mask_raster_with_shapefile(merged_raster_path, region_geometry, masked_raster_path)

        if not final_mask:
            logger.warning(f"Masked raster not created for region {region_code}.")
            return

        # Delete intermediate rasters
        for rp in downloaded_rasters:
            try:
                os.remove(rp)
                logger.debug(f"Deleted intermediate file: {rp}")
            except OSError as e:
                logger.error(f"Failed to delete intermediate file {rp}: {e}")

        try:
            os.remove(merged_raster_path)
            logger.debug(f"Deleted merged raster: {merged_raster_path}")
        except OSError as e:
            logger.error(f"Failed to delete merged raster {merged_raster_path}: {e}")

        logger.info(f"Completed processing for region {region_code}.")

    # Process regions using threads
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
                logger.error(f"Error in a task: {e}")

    logger.info("Download completed.")

# Example usage
if __name__ == "__main__":
    # Initialize Earth Engine
    initialize_gee("path/to/credentials.json")

    # Load your GeoDataFrame
    gdf = gpd.read_file("path/to/shapefile.shp")
    gdf = gdf[gdf['iso_3166_1_'] == 'ES']  # Example filter for Spain

    # Parameters
    collection_id = "MODIS/061/MCD12Q1"
    band_name = "LC_Type1"

    # Download data
    download_gee_data_by_gdf(
        gdf=gdf,
        column_name="iso_3166_1_",  # Adjust based on your shapefile's unique identifier column
        collection=collection_id,
        band=band_name,
        output_folder="./gee_downloads",
        start_date="2001-01-01",      # Set to None if not applicable
        end_date="2001-12-31",        # Set to None if not applicable
        scale=500,
        max_workers=5,
    )
