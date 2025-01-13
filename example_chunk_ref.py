import xarray as xr
import numpy as np
import pandas as pd

import os
import sys
import yaml
from datetime import datetime
from xclim import sdba

import dask
from dask.distributed import Client, LocalCluster, wait


# set as appropriate, though this is an intermediate step so should be scratch or other temp location
chunkdir = "../example/zarr_in/"
    

# AGCD data uses slightly different variable names
agcd_to_standard = {"precip": "pr", "tmax": "tasmax", "tmin": "tasmin"}


def standardise_latlon(ds, digits=2):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat.astype('float64'), digits)}) # after xarray version update, convert first before rounding
    ds = ds.assign_coords({"lon": np.round(ds.lon.astype('float64'), digits)})
    return(ds)


def preprocess_ds(ds):
    # set errant time values to datetime (no-leap models will have conflicting time values otherwise)
    if ds.time.dtype == "O":
        ds = ds.assign_coords(time = ds.indexes['time'].to_datetimeindex(True))
    
    ds = standardise_latlon(ds)

    for key in agcd_to_standard:
        if key in ds.variables:
            ds = ds.rename({key: agcd_to_standard[key]})

    ds = ds.drop_vars(["time_bnds", "lat_bnds", "lon_bnds"], errors = 'ignore')
    ds = ds.drop_dims("bnds", errors = 'ignore')

    # round all time values to midday for consistency
    # may not be necessary for univariate methods, depending on the gcm and ref data
    ds = ds.assign_coords(time = ds.time.dt.floor("D") + np.timedelta64(12, 'h')) 

    # jitter 
    if "pr" in ds.variables:
        ds["pr"] = sdba.processing.jitter(ds.pr, lower="0.01 mm d-1",minimum="0 mm d-1")

    return ds


if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])

    train_yr_start = 1960
    train_yr_end = 1989

    chunk_size = 20
    all_vars = ["pr", "tasmax", "tasmin"]
    
    ref_file_list = []
    
    ref_path = '/g/data/ia39/npcp/data/{var}/observations/AGCD/raw/task-reference/{var}_NPCP-20i_AGCD_v1-0-1_day_{year}0101-{year}1231.nc'    
    ref_file_list = [ref_path.format(var=var, year=y) for y in range(train_yr_start, train_yr_end + 1) for var in all_vars]

    data_full = xr.open_mfdataset(ref_file_list, preprocess = preprocess_ds, parallel = True).persist()
    data_stacked = data_full[all_vars].to_array(dim = "s_vars")
    data_ds = xr.Dataset({"data": data_stacked}).chunk(time = -1, s_vars = -1, lat = chunk_size, lon = chunk_size).persist()

    if not os.path.exists(chunkdir):
        os.makedirs(chunkdir)

    path = chunkdir + f'ref-{train_yr_start}-{train_yr_end}-{"-".join(all_vars)}'
    data_ds.to_zarr(path, consolidated = True, mode = 'w')
