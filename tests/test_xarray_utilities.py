import sys
import types

for mod in [
    "dask",
    "dask.array",
    "rasterio",
    "rasterio.crs",
    "rioxarray",
    "xarray",
    "zarr",
    "zarr.core",
]:
    module = sys.modules.setdefault(mod, types.ModuleType(mod))
    if mod == "rasterio.crs":
        setattr(module, "CRS", object)
    if mod == "xarray":
        # provide a minimal stub for DataArray used only for type hints
        class _DummyDataArray:
            pass

        setattr(module, "DataArray", _DummyDataArray)
    if mod == "zarr" or mod == "zarr.core":
        setattr(module, "array", object)
        if mod == "zarr":
            core_mod = sys.modules.setdefault("zarr.core", types.ModuleType("zarr.core"))
            setattr(core_mod, "Array", object)
            setattr(module, "core", core_mod)

from affine import Affine
from src.utilities.xarray_utilities import affine_has_rotation


def test_affine_has_rotation_false():
    aff = Affine.translation(1, 2) * Affine.scale(3, 4)
    assert not affine_has_rotation(aff)


def test_affine_has_rotation_true_rotation():
    aff = Affine.rotation(30)
    assert affine_has_rotation(aff)


def test_affine_has_rotation_true_shear():
    # shear matrix: shear in x-direction using b
    a = 1
    b = 0.1
    d = 0
    e = 1
    aff = Affine(a, b, 0, d, e, 0)
    assert affine_has_rotation(aff)
