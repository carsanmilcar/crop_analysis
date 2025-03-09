import geopandas as gpd
from shapely.geometry import box  # Corrected import
from typing import List, Union, Optional, Dict
import os
import numpy as np
import xarray as xr
from dask import delayed
from dataset_generation.utilities.download_utilities import download_file
import dask.array as da
from rasterio import features
from affine import Affine
from pyproj import Transformer
from dataset_generation.utilities.xarray_utilities import (
    affine_to_coords,
    normalize_array,
)
import zipfile
import urllib.parse

def filter_countries_by_attribute(
    world: gpd.GeoDataFrame,
    attribute: str,
    values: List[Union[str, int, float]],
    exact_match: bool = True,
    bounding_box: Optional[gpd.GeoDataFrame] = None,
) -> gpd.GeoDataFrame:
    """
    Filters the GeoDataFrame based on arbitrary attribute values and optionally limits to a bounding box.

    Args:
        world (gpd.GeoDataFrame): Original GeoDataFrame containing world countries.
        attribute (str): Column name to filter by.
        values (List[Union[str, int, float]]): List of values to filter.
        exact_match (bool, optional): If True, performs exact matching.
                                      If False, performs partial matching for strings. Defaults to True.
        bounding_box (Optional[gpd.GeoDataFrame]): Optional GeoDataFrame containing a bounding box.

    Returns:
        gpd.GeoDataFrame: Filtered GeoDataFrame containing only specified countries within the bounding box.

    Raises:
        KeyError: If the attribute column is not found in the GeoDataFrame.
        ValueError: If CRS mismatch occurs between the GeoDataFrame and bounding box.
    """
    # Check if attribute exists in the GeoDataFrame
    if attribute not in world.columns:
        raise KeyError(
            f"Attribute '{attribute}' not found in the shapefile. "
            f"Available columns: {world.columns.tolist()}"
        )

    # Perform filtering by attribute
    if exact_match:
        filtered = world[world[attribute].isin(values)].drop_duplicates(
            subset=[attribute]
        )
    else:
        # For partial matching, assuming the attribute is of string type
        mask = (
            world[attribute]
            .astype(str)
            .str.contains("|".join(values), case=False, na=False)
        )
        filtered = world[mask].drop_duplicates(subset=[attribute])

    if filtered.empty:
        raise ValueError(
            f"No countries matched the attribute '{attribute}' with values {values}."
        )

    # Use the bounding box if provided
    if bounding_box is not None:
        if not world.crs.equals(bounding_box.crs):
            raise ValueError(
                f"CRS mismatch: GeoDataFrame CRS is '{world.crs}', but bounding box CRS is '{bounding_box.crs}'."
            )
        filtered = gpd.clip(filtered, bounding_box)
    else:
        # Automatically compute bounding box from filtered geometries
        minx, miny, maxx, maxy = filtered.total_bounds

    return filtered


def create_bounding_box(
    min_x: float, min_y: float, max_x: float, max_y: float, crs: str
) -> gpd.GeoDataFrame:
    """
    Creates a GeoDataFrame containing a single bounding box geometry.

    Args:
        min_x (float): Minimum X coordinate.
        min_y (float): Minimum Y coordinate.
        max_x (float): Maximum X coordinate.
        max_y (float): Maximum Y coordinate.
        crs (str, optional): Coordinate Reference System.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with the bounding box.
    """
    bbox = box(min_x, min_y, max_x, max_y)
    bbox_gdf = gpd.GeoDataFrame({"geometry": [bbox]}, crs=crs)

    return bbox_gdf


def rasterize_countries(
    gdf: gpd.GeoDataFrame,
    values: List[str],
    attribute: str,
    resolution: float,  # Mandatory parameter
    bounding_box: Optional[gpd.GeoDataFrame] = None,  # Optional bounding box
    naming_pattern: Optional[str] = None,
    adjust_boundaries: bool = True,  # Whether to adjust boundaries to resolution
) -> xr.Dataset:
    """
    Rasterizes each category based on category codes and aggregates them into an xarray.Dataset.

    Args:
        gdf (gpd.GeoDataFrame): GeoDataFrame in the target CRS.
        values (List[str]): List of category codes to rasterize.
        attribute (str): Column name for category codes in the GeoDataFrame.
        resolution (float): Pixel resolution (e.g., in degrees or meters).
        bounding_box (Optional[gpd.GeoDataFrame]): Bounding box GeoDataFrame to limit the extent.
        naming_pattern (Optional[str]):
            String pattern to generate names for raster layers.
            Use `{code}` and/or `{attribute}` as placeholders.
            Example: "country_{code}"
            If None, defaults to "{attribute}_{code}".
        adjust_boundaries (bool): If True, adjusts boundaries to align with the resolution.

    Returns:
        xr.Dataset: Dataset containing rasterized categories.
    """
    # Filter the GeoDataFrame for the selected categories
    filtered_gdf = gdf[gdf[attribute].isin(values)]
    if filtered_gdf.empty:
        raise ValueError("No geometries found for the specified category codes.")

    # If no bounding box is provided, compute from the filtered geometries
    if bounding_box is None:
        minx, miny, maxx, maxy = filtered_gdf.total_bounds
    else:
        # Ensure CRS consistency if a bounding box is provided
        if not gdf.crs.equals(bounding_box.crs):
            raise ValueError(
                f"CRS mismatch: GeoDataFrame CRS is '{gdf.crs}', "
                f"but bounding box CRS is '{bounding_box.crs}'."
            )
        # Clip the GeoDataFrame to the bounding box
        filtered_gdf = gpd.clip(filtered_gdf, bounding_box)
        minx, miny, maxx, maxy = bounding_box.total_bounds

    # Adjust boundaries to align with resolution
    if adjust_boundaries:
        minx = resolution * (minx // resolution)
        miny = resolution * (miny // resolution)
        maxx = resolution * (maxx // resolution + 1)
        maxy = resolution * (maxy // resolution + 1)

    # Compute raster dimensions
    ncols = int((maxx - minx) / resolution)
    nrows = int((maxy - miny) / resolution)

    # Generate transform
    transform = Affine.translation(minx, maxy) * Affine.scale(resolution, -resolution)

    # Compute chunk size to ensure maximum of 100 MB
    dtype = np.int32  # Assumes int32 data type; adjust if necessary
    bytes_per_pixel = np.dtype(dtype).itemsize
    max_chunk_bytes = 100 * 1024 * 1024  # 100 MB
    max_pixels_per_chunk = max_chunk_bytes // bytes_per_pixel
    chunk_side_length = int(max_pixels_per_chunk**0.5)
    chunk_size = (min(chunk_side_length, nrows), min(chunk_side_length, ncols))

    country_arrays: Dict[str, xr.DataArray] = {}

    # Generate y and x coordinates
    coords = affine_to_coords(transform, ncols, nrows)
    x_coords = coords["x"]
    y_coords = coords["y"]

    for code in values:
        # Filter the current category
        category_shape = filtered_gdf[filtered_gdf[attribute] == code]
        if category_shape.empty:
            continue

        # Prepare shapes for rasterization
        shapes = list(zip(category_shape.geometry, [1] * len(category_shape)))

        # Define a delayed rasterization function
        @delayed
        def rasterize_category(shapes, out_shape, transform):
            return features.rasterize(
                shapes=shapes,
                out_shape=out_shape,
                fill=0,
                transform=transform,
                dtype="int32",
            )

        # Create a Dask array from the delayed rasterization
        category_array = da.from_delayed(
            rasterize_category(shapes, (nrows, ncols), transform),
            shape=(nrows, ncols),
            dtype="int32",
        ).rechunk(chunk_size)

        # Create an xarray DataArray
        da_child = xr.DataArray(
            data=category_array,
            dims=["y", "x"],
            coords={"y": y_coords, "x": x_coords},
            attrs={"crs": f"EPSG:{gdf.crs.to_epsg()}" if gdf.crs else "unknown"},
        )

        # Normalize the DataArray to ensure consistency
        da_child_normalized = normalize_array(da_child)

        # Generate the layer name
        if naming_pattern:
            try:
                layer_name = naming_pattern.format(code=code, attribute=attribute)
            except KeyError:
                layer_name = f"{attribute}_{code}"
        else:
            layer_name = f"{attribute}_{code}"

        country_arrays[layer_name] = da_child_normalized

    # Construct the xarray.Dataset
    country_codes_ds = xr.Dataset(
        data_vars=country_arrays,
        coords={"y": ("y", y_coords), "x": ("x", x_coords)},
    )
    country_codes_ds.attrs["crs"] = (
        f"EPSG:{gdf.crs.to_epsg()}" if gdf.crs else "unknown"
    )

    return country_codes_ds
