import xarray as xr
import numpy as np

import os
import sys
import yaml
from datetime import datetime
import cmdline_provenance as cmdprov

import shutil

import dask
from dask.distributed import Client, LocalCluster, wait

# should match the calcdir of acs_calc_mrnbc and acs_repair_mrnbc
calcdir = "/scratch/eg3/ag0738/mrnbc/example/zarr_out/"

# output directory path
outdir = "/scratch/eg3/ag0738/mrnbc/example/converted/"
# outdir = "/g/data/ia39/australian-climate-service/test-data/CORDEX-CMIP6/bias-adjustment-output/AGCD-05i/"


def standardise_latlon(ds, digits=2):
    """
    This function rounds the latitude / longitude coordinates to the 4th digit, because some dataset
    seem to have strange digits (e.g. 50.00000001 instead of 50.0), which prevents merging of data.
    """
    ds = ds.assign_coords({"lat": np.round(ds.lat.astype('float64'), digits)}) # after xarray version update, convert first before rounding
    ds = ds.assign_coords({"lon": np.round(ds.lon.astype('float64'), digits)})
    return(ds)


def adjust_attrs(ds, var):
    
    ds = ds.assign_attrs({"bc_vars": ", ".join(all_vars)})
    ds = ds.assign_attrs({"creation_date": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')})
    
    # variable specific
    ds[var].attrs['long_name'] = f"Bias-Adjusted {ds[var].attrs['long_name']}"
    ds = ds.rename({var: f'{var}Adjust'})

    ds.attrs['history'] = cmdprov.new_log() + '\n' + ds.attrs['history']
    
    return ds
    

if __name__ == "__main__":
    client = Client(scheduler_file = os.environ["DASK_PBS_SCHEDULER"])
    
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
    
    # same section logic used in chunking part
    section_starts = [apply_yr_start]
    section_length = train_yr_end - train_yr_start + 1

    end_point = apply_yr_end - section_length + 1
    
    year = apply_yr_start + section_length
    while year < end_point:
        if year > train_yr_start and year < train_yr_end:
            year = train_yr_start
            
        section_starts.append(year)
        year += section_length

    if end_point not in section_starts:
        section_starts.append(end_point)
    
    # for metadata
    gcm_path = '/g/data/ia39/npcp/data/{var}/{gcm}/{org}-{rcm}/raw/task-reference/{var}_NPCP-20i_{gcm}_{empat}_{mdl_run}_{org}-{rcm}_v1_day_{year}0101-{year}1231.nc'

    mdl_file_list = {var: [gcm_path.format(var=var, year=y, gcm = gcm, mdl_run = mdl_run, org = org, rcm = rcm, empat = 'historical' if y <= 2014 else empat) for y in range(apply_yr_start, apply_yr_end + 1)] for var in all_vars}

    # load in and correct files one by one
    # doing it this way may be slightly slower but ensures accuracy of metadata lost along the way without having to hard-code workarounds (and bnds)
    tasks = []
    section = 0
    bc_data = None
    for year in range(apply_yr_start, apply_yr_end + 1):

        # load the next section
        if section < len(section_starts):
            if year == section_starts[section]:
                if tasks:
                    dask.compute(*tasks)
                tasks = []
                
                if bc_data:
                    bc_data.close()
                    shutil.rmtree(bc_data_path)
                    
                yr_start = section_starts[section]
                yr_end = yr_start + section_length - 1
                bc_data_path = calcdir + f"{org}/{gcm}/{mdl_run}/{rcm}/{yr_start}-{yr_end}"
                if os.path.isdir(bc_data_path):
                    bc_data = xr.open_zarr(bc_data_path, chunks = {"lat": -1, "lon": -1, "time": 'auto'})
                    print(f"Converting {yr_start}-{yr_end}...")
                else:
                    print(f"Missing {yr_start}-{yr_end} - did it already get converted?")
                    bc_data = None
                section += 1

        if bc_data:
            bc_data_year = bc_data.sel(time = (bc_data.time.dt.year == year))
            for var in all_vars:
        
                path = outdir + f'{org}/{gcm}/{mdl_run}/{rcm}/MRNBC-{train_yr_start}-{train_yr_end}/day/{var}Adjust/'
                if not os.path.exists(path):
                    os.makedirs(path)
                
                mdl_data = xr.open_dataset(mdl_file_list[var][year - apply_yr_start])
    
                # assign bias corrected data to take the place of original data
                mdl_corrected = mdl_data.assign({var: bc_data_year[var].assign_coords(time = mdl_data.time, lat = mdl_data.lat, lon = mdl_data.lon).transpose("time", "lat", "lon")})
                mdl_corrected[var].attrs.update(mdl_data[var].attrs)
    
                mdl_corrected = adjust_attrs(mdl_corrected, var)
    
                task = mdl_corrected.to_netcdf(path + f'{var}Adjust_NPCP_{gcm}_{"historical" if year <= 2014 else empat}_{mdl_run}_{org}_{rcm}_MRNBC-{train_yr_start}-{train_yr_end}_day_{year}0101-{year}1231.nc', 
                                               encoding={var + 'Adjust': {'chunksizes': (1, mdl_corrected.lat.size, mdl_corrected.lon.size), 'least_significant_digit': 2, 'zlib': True, 'complevel': 5}},
                                               compute = False)
    
                tasks.append(task)

    dask.compute(*tasks)
                
    if bc_data:
        bc_data.close()
        shutil.rmtree(bc_data_path)
