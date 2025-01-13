import xarray as xr
import numpy as np
import pandas as pd

import os
import sys
import yaml
from datetime import datetime

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

    train_yr_start = 1960
    train_yr_end = 1989

    apply_yr_start = 1960
    apply_yr_end = 2019
    
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

    # load reference and historical data
    ref_data = xr.open_zarr(chunkdir + f'ref-{train_yr_start}-{train_yr_end}-{"-".join(all_vars)}')
    hist_data = xr.open_zarr(chunkdir + f'{org}/{gcm}/{mdl_run}/{rcm}/{train_yr_start}-{train_yr_end}-{"-".join(all_vars)}')

    # preprocess input data before feeding into the Fortran module
    ref_arr = prepare_input_array(ref_data["data"], train_yr_start, section_length, nvars).persist()
    hist_arr = prepare_input_array(hist_data["data"], train_yr_start, section_length, nvars).persist()

    # flag for whether or not to save hist_bc (bias corrected mdl training data), this will be set to False after the first run if there are multiple runs
    first_run = True

    for run_yr_start in section_starts:
        run_yr_end = run_yr_start + section_length - 1

        # load future data
        fut_data = xr.open_zarr(chunkdir + f'{org}/{gcm}/{mdl_run}/{rcm}/{run_yr_start}-{run_yr_end}-{"-".join(all_vars)}')
        fut_arr = prepare_input_array(fut_data["data"], run_yr_start, section_length, nvars).persist()

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

        # leap year + strange calendar check
        # will add filters to remove extra days to data_config if necessary
        data_config = get_index_filters(ref_data, hist_data, fut_data, data_config)

        # perform MRNBC bias correction
        hist_bc_arr, fut_bc_arr = do_mrnbc(ref_arr, hist_arr, fut_arr, data_config, module)

        # fix time array values using original dataset values
        hist_bc = hist_bc_arr.to_dataset('s_vars').assign_coords({'timec': hist_data.time.data})
        hist_bc = hist_bc.rename({'timec': 'time'}).persist()
        
        fut_bc = fut_bc_arr.to_dataset('s_vars').assign_coords({'timef': fut_data.time.data})
        fut_bc = fut_bc.rename({'timef': 'time'}).persist()
    
        wait([hist_bc, fut_bc])
    
        hist_bc = hist_bc.chunk(lat = -1, lon = -1, time = 1)
        fut_bc = fut_bc.chunk(lat = -1, lon = -1, time = 1)

        # create DRS if required
        output_path = calcdir + f'{org}/{gcm}/{mdl_run}/{rcm}/'
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        # only save corrected historical data the first time as it should be identical on following runs
        if first_run:
            hist_bc.to_zarr(output_path + f'{train_yr_start}-{train_yr_end}', consolidated = True, mode = 'w')
            first_run = False

        # save corrected future data
        fut_bc.to_zarr(output_path + f'{run_yr_start}-{run_yr_end}', consolidated = True, mode = 'w')

