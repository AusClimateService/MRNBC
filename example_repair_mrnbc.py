import xarray as xr
import numpy as np
import pandas as pd

import os
import sys
import yaml
from datetime import datetime
import time

import random

import dask
from dask.distributed import Client, LocalCluster, wait

from pymrnbc import *
import module

# set as appropriate, though this is an intermediate step so should be scratch or other temp location
# chunkdir should match the directory given in acs_chunk_mrnbc and acs_chunk_ref 
chunkdir = "/scratch/eg3/ag0738/mrnbc/example/zarr_in/"
calcdir = "/scratch/eg3/ag0738/mrnbc/example/zarr_out/"


def standardise_latlon(ds, digits=2):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat.astype('float64'), digits)}) # after xarray version update, convert first before rounding
    ds = ds.assign_coords({"lon": np.round(ds.lon.astype('float64'), digits)})
    return(ds)


if __name__ == "__main__":
    
    # Set up Dask cluster from the scheduler
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])
    client.upload_file("module.cpython-310-x86_64-linux-gnu.so")
    client.upload_file("pymrnbc.py")
    
    dask.config.set({'logging.distributed': 'error'})
    
    gcm = "CSIRO-ACCESS-ESM1-5"
    empat = "ssp370"
    mdl_run = "r6i1p1f1"
    org = "BOM"
    rcm = "BARPA-R"

    all_vars = ["pr", "tasmax", "tasmin"]

    chunk_size = 20

    train_yr_start = 1960
    train_yr_end = 1989

    apply_yr_start = 1960
    apply_yr_end = 2019
    
    nvars = len(all_vars)

    # DEBUG
    time_start = time.time()

    nvars = len(all_vars)

    # in the format [lower limit, upper limit, aggregate flag (0 for avg, 1 for sum), threshold flag, threshold value if flagged]
    var_details = {
        "pr": [0, 1000, 1, 1, 1],
        "tasmax": [-20, 65, 0, 0, 1],
        "tasmin": [-20, 65, 0, 0, 1],
        "rsds": [0, 500, 0, 0, 1],
        "sfcWindmax": [0, 100, 0, 0, 1],
        "hursmax": [0, 100, 0, 0, 1],
        "hursmin": [0, 100, 0, 0, 1]
    }
    
    # same section logic used in chunking part
    section_length = train_yr_end - train_yr_start + 1
    section_starts = get_sections(train_yr_start, train_yr_end, apply_yr_start, apply_yr_end)

    # after having calculated the sections, remove the one that is already covered by historical training data
    if train_yr_start in section_starts:
        section_starts.remove(train_yr_start)

    print(f"DEBUG: Opening zarrs at {time.time() - time_start}")
    
    ref_data = xr.open_zarr(chunkdir + f'ref-{train_yr_start}-{train_yr_end}-{"-".join(all_vars)}', chunks = {"time": -1, "s_vars": -1, "lat": chunk_size, "lon": chunk_size})
    hist_data = xr.open_zarr(chunkdir + f'{org}/{gcm}/{mdl_run}/{rcm}/{train_yr_start}-{train_yr_end}-{"-".join(all_vars)}', chunks = {"time": -1, "s_vars": -1, "lat": chunk_size, "lon": chunk_size})

    section = 0
    attempts = 0
    while section < len(section_starts):
        run_yr_start = section_starts[section]
        run_yr_end = run_yr_start + section_length - 1

        output_path = calcdir + f'{org}/{gcm}/{mdl_run}/{rcm}/'
        prev_path = output_path + f'{run_yr_start}-{run_yr_end}'

        if not os.path.isdir(prev_path):
            print(f"Missing {run_yr_start}-{run_yr_end} - did it already get fixed/converted?")
            section += 1
            attempts = 0
            continue
        
        prev_data = xr.open_zarr(prev_path, chunks = {"lat": -1, "lon": -1, "time": 'auto'})

        print(f"DEBUG: Starting checks at {time.time() - time_start}")

        random_time = random.randrange(prev_data.time.size - 1)

        # take slices from first and last vars to see if there are problems (maxed values)
        comparison_first_var = (prev_data[all_vars[0]].isel(time = -1, drop = True) == var_details[all_vars[0]][1]).compute()
        comparison_last_var = (prev_data[all_vars[-1]].isel(time = -1, drop = True) == var_details[all_vars[-1]][1]).compute()
        comparison_last_var_2 = (prev_data[all_vars[-1]].isel(time = random_time, drop = True) == var_details[all_vars[-1]][1]).compute()
        comparison_day = comparison_first_var.where(comparison_first_var, other = comparison_last_var.where(comparison_last_var_2, other = False))
        
        bad_locs = comparison_day.where(comparison_day, other = np.nan, drop = True)
        
        prev_data.close()
        
        print(f'Fixing {run_yr_start}-{run_yr_end}: {bad_locs.size}')

        # skip as the data is already correct or there have been too many attempts to fix it (i.e. might not actually be erroneous)
        if bad_locs.size == 0 or attempts > 3:
            section += 1
            attempts = 0
            continue

        attempts += 1

        tasks = []
        
        # search each chunk for problem values
        for lat_start in range(0, comparison_day.lat.size, chunk_size):
            for lon_start in range(0, comparison_day.lon.size, chunk_size):
                lat_end = min(lat_start + chunk_size, comparison_day.lat.size)
                lon_end = min(lon_start + chunk_size, comparison_day.lon.size)
                lat_slice = slice(lat_start, lat_end)
                lon_slice = slice(lon_start, lon_end)
                comparison_chunk = comparison_day.isel(lat = lat_slice, lon = lon_slice)
                erred_values = comparison_chunk.sum().compute().item()

                # no problems, skip and move onto next
                if erred_values == 0:
                    continue

                # one data point problem - select that one single point to recorrect
                elif erred_values == 1:
                    erred_loc = comparison_chunk.where(comparison_chunk, drop = True)
                    lat_slice = np.where(comparison_day.lat.values == erred_loc.lat.values[0])[0]
                    lon_slice = np.where(comparison_day.lon.values == erred_loc.lon.values[0])[0]

                # else - proceed to recorrect the whole chunk

                fut_data = xr.open_zarr(chunkdir + f'{org}/{gcm}/{empat}/{mdl_run}/{rcm}/{run_yr_start}-{run_yr_end}-{"-".join(all_vars)}').isel(lat = lat_slice, lon = lon_slice)

                ref_arr = prepare_input_array(ref_data["data"].isel(lat = lat_slice, lon = lon_slice), train_yr_start, section_length, nvars)
                hist_arr = prepare_input_array(hist_data["data"].isel(lat = lat_slice, lon = lon_slice), train_yr_start, section_length, nvars)
                fut_arr = prepare_input_array(fut_data["data"], run_yr_start, section_length, nvars)

                # parameters for passing around Python or Fortran functions
                # should not need to be modified
                data_config = { 
                    "nyh": section_length,   # number of years of observed data
                    "nsh": train_yr_start, # Start year for observed data
                    "nyc": section_length,   # number of years of gcm historical data
                    "nsc": train_yr_start, # start year for GCM historical data
                    "nyf": section_length,   # number of gcm future data
                    "nsf": run_yr_start, # start year for GCM future data
                    "leapcode": 0, # Number of days in leap years # 0: 366 days # 1: 365 days
                    "itime": 0, # Timescale Daily (0) or monthly (1)
                    "nyrmax": section_length,
                    "ndaymax": 366,
                    "monmax": 13,
                    "ndmax": 31,
                    "nsmax": 6,
                    "nvarmax": nvars,
                    "mvmax": 31,
                    "miss": -9000, # Default value for missing values
                    "fillval": -9990.0, # Default value for padded arrays for fmrnbc
                    "all_vars": all_vars,
                    "var_details": var_details
                }    
        
                # leap year check (to see if leap days are included)
                # if they are not, they will need to be filtered out from the mrnbc output by index
                data_config = get_index_filters(ref_data, hist_data, fut_data, data_config)
        
                # perform MRNBC bias correction
                hist_bc_arr, fut_bc_arr = do_mrnbc(ref_arr, hist_arr, fut_arr, data_config, module)

                # fix time array values using original dataset values
                hist_bc = hist_bc_arr.to_dataset('s_vars').assign_coords({'timec': hist_data.time.data})
                hist_bc = hist_bc.rename({'timec': 'time'})
                
                fut_bc = fut_bc_arr.to_dataset('s_vars').assign_coords({'timef': fut_data.time.data})
                fut_bc = fut_bc.rename({'timef': 'time'})
            
                fut_bc = fut_bc.chunk(lat = -1, lon = -1, time = 1)

                fut_bc.to_zarr(output_path + f'{run_yr_start}-{run_yr_end}', consolidated = True, mode = 'r+', region = 'auto')

