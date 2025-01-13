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

from pymrnbc import get_sections


# set as appropriate, though this is an intermediate step so should be scratch or other temp location   
chunkdir = "/scratch/eg3/ag0738/mrnbc/example/zarr_in/"
    

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

    ds = ds.drop_vars(["time_bnds", "lat_bnds", "lon_bnds", "height"], errors = 'ignore')
    ds = ds.drop_dims("bnds", errors = 'ignore')

    # round all time values to midday for consistency
    ds = ds.assign_coords(time = ds.time.dt.floor("D") + np.timedelta64(12, 'h')) 

    return ds


if __name__ == "__main__":

    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])
    client.upload_file("pymrnbc.py")

    gcm = "CSIRO-ACCESS-ESM1-5"
    empat = "ssp370"
    mdl_run = "r6i1p1f1"
    org = "BOM"
    rcm = "BARPA-R"
    
    chunk_size = 20
    all_vars = ["pr", "tasmax", "tasmin"]

    train_yr_start = 1960
    train_yr_end = 1989

    apply_yr_start = 1960
    apply_yr_end = 2019

    
    # calculate the different runs that will be required
    # one of these sections will be the correction applied to the training data (i.e. hist_bc) and will always be calculated
    # the rest will cover the gaps with run lengths equal to the training period, potentially including overlap, and each will need its own run
    # same section logic used in chunking part
    section_length = train_yr_end - train_yr_start + 1
    section_starts = get_sections(train_yr_start, train_yr_end, apply_yr_start, apply_yr_end)
    
    gcm_path = '/g/data/ia39/npcp/data/{var}/{gcm}/{org}-{rcm}/raw/task-reference/{var}_NPCP-20i_{gcm}_{empat}_{mdl_run}_{org}-{rcm}_v1_day_{year}0101-{year}1231.nc'

    chunkdir = chunkdir + f'{org}/{gcm}/{mdl_run}/{rcm}/'
    if not os.path.exists(chunkdir):
        os.makedirs(chunkdir)

    for year_start in section_starts:
        year_end = year_start + section_length - 1
        file_list = [gcm_path.format(var=v, year=y, gcm = gcm, mdl_run = mdl_run, org = org, rcm = rcm, empat = 'historical' if y <= 2014 else empat) for v in all_vars for y in range(year_start, year_end + 1)]
        
        data_full = xr.open_mfdataset(file_list, preprocess = preprocess_ds, parallel = True).persist()
        
        data_stacked = data_full[all_vars].to_array(dim = "s_vars")
        data_ds = xr.Dataset({"data": data_stacked}).chunk(time = -1, s_vars = -1, lat = chunk_size, lon = chunk_size).persist()
        
        data_ds.to_zarr(chunkdir + f'{year_start}-{year_end}-{"-".join(all_vars)}', consolidated = True, mode = 'w')
        
        data_ds.close()

