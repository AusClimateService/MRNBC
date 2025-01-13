import xarray as xr
import numpy as np
import pandas as pd

import os
import sys
import yaml
from datetime import datetime

import dask
from dask.distributed import Client, LocalCluster, wait

# from module import mrnbc


def get_sections(train_yr_start, train_yr_end, apply_yr_start, apply_yr_end):
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

    return section_starts


def get_index_filters(ref_data, hist_data, fut_data, data_config, cal360 = False):

    # leap year check (to see if leap days are included)
    # if they are not, they will need to be filtered out from the mrnbc output by index
    index_filter = [i for i, item in enumerate(ref_data.time.values) if item in hist_data.time.values]
    index_filter_fut = list(range(fut_data.time.size))
    
    if len(index_filter) != ref_data.time.size:
        data_config['leapcode'] = 1
        
        for year in range(run_yr_start, run_yr_end + 1):
            if year % 4 == 0 and year != 2100:
                start_point = (year - run_yr_start) * 365 + 59
                for i in range(start_point, len(index_filter_fut)):
                    index_filter_fut[i] += 1

        if cal360:
            increment_amount = 0
            for i in range(1, len(index_filter_fut)):
                if i % 360 in (0, 150, 210, 240, 300):
                    increment_amount += 1
                index_filter_fut[i] += increment_amount

    data_config['index_filter'] = index_filter
    data_config['index_filter_fut'] = index_filter_fut

    data_config['timec'] = len(index_filter)
    data_config['timef'] = len(index_filter_fut)

    return data_config


def create_array(clim_data, times, startyr, nyr, nvar=5, nmon=12, nday=31):
    """Function to process DataArray location at a time
    Generate 4D array for MRNBC

    input: clim_data -> (ndate, nvar)
    output: (nvar, nyr, nmon, nday)""" 

    # DataFrame to reindex by year, month, and day
    df = pd.DataFrame(data=clim_data)
    df['time'] = pd.to_datetime(times)
    df.loc[:, 'year'] = df.time.apply(lambda x: x.year)
    df.loc[:, 'month'] = df.time.apply(lambda x: x.month)
    df.loc[:, 'day'] = df.time.apply(lambda x: x.day)
    df = df.drop(columns=['time'])
    df = df.set_index(['year', 'month', 'day'])

    # Grid for all dates
    grid = np.mgrid[:nyr, :nmon, :nday]
    x1 = grid[0].ravel() + startyr
    x2 = grid[1].ravel() + 1
    x3 = grid[2].ravel() + 1 

    # Reindex and convert back to array
    arr = df.reindex(
        pd.MultiIndex.from_tuples(np.vstack([x1, x2, x3]).T.tolist())
    ).T.values.reshape((nvar, nyr, nmon, nday))

    # Handle null values    
    for i in range(nvar):
        meanval = np.nanmean(arr[i])
        arr[i] = np.nan_to_num(arr[i], nan=meanval)
    
    return arr


def prepare_input_array(clim_data, year_start, year_count, nvars):

    # days and mons
    nmon = 13
    nday = 31

    # Arguments to ufunc
    kwargs = {'times': clim_data.time.data,
              'startyr': year_start, 'nyr': year_count,
              'nvar': nvars, 'nmon': nmon, 'nday': nday}

    # Output dimensions from ufunc
    output_sizes = {"year": year_count, "month": nmon, 
                    "day": nday, "s_vars": nvars}

    # Apply ufunc to convert data to 4d arrays for MRNBC
    clim_data_arr = xr.apply_ufunc(create_array, clim_data, 
                                   input_core_dims=[["time", "s_vars"]], 
                                   output_core_dims=[["s_vars", "year", 
                                                      "month", "day"]],
                                   vectorize=True, dask='parallelized',
                                   kwargs=kwargs, output_dtypes=[np.float32],
                                   dask_gufunc_kwargs={"output_sizes": 
                                                       output_sizes})
    return clim_data_arr


def do_mrnbc(ref_arr, hist_arr, fut_arr, data_config, module):    
    
    output_sizes = {"timec": data_config['timec'], "timef": data_config['timef']}

    leap = [data_config['leapcode']]*4
    nday = np.zeros(data_config['monmax'])
    nday[:12] = np.array([31, 29, 31, 30,
                          31, 30, 31, 31,
                          30, 31, 30, 31], order='F')
    
    idays = np.zeros((data_config['monmax'], 4))
    if data_config['itime'] == 0:
        iband = 11
        isum = sum(leap)
        if isum > 0:
            nday[1] = 28
            idays = np.repeat(nday, 4).reshape((-1, 4))
    else:
        iband = None
    
    def get_params(ref, hist, fut, data_config):
    
        # Maximum size configuration
        NYRMAX = data_config['nyrmax']
        MONMAX = data_config['monmax']
        NDMAX = data_config['ndmax']
        NVARMAX = data_config['nvarmax']
    
        # theoretically the same as NVARMAX these days but that could be changed depending on config
        nvars = len(data_config['all_vars'])
    
        phlwr, phupr, igg, ilimit, thres = np.array([data_config['var_details'][v] for v in data_config['all_vars']], dtype = float).T
        thres[thres==0] = 0.00000001
    
        # Create arrays to store output
        nrows = NYRMAX*MONMAX*NDMAX
        if data_config['itime']==0:
            ncols = NVARMAX + 3
        else:
            ncols = NVARMAX + 2
        
        # Arrays to store bias corrected output from mrnbc
        gcmcbc = np.ones((nrows, ncols), 'd', order='F') * data_config['fillval']
        gcmfbc = np.ones((nrows, ncols), 'd', order='F') * data_config['fillval']
    
        # Create zeros padded arrays for inputs
        # Array size is given by constants
        # The constants dictate maximum number of years, months, days and vars
        gcmc = np.zeros((NVARMAX, NYRMAX, MONMAX, NDMAX), 'd', order='F')
        gcmf = np.zeros((NVARMAX, NYRMAX, MONMAX, NDMAX), 'd', order='F')
        obsc = np.zeros((NVARMAX, NYRMAX, MONMAX, NDMAX), 'd', order='F')
        obsf = np.zeros((NVARMAX, NYRMAX, MONMAX, NDMAX), 'd', order='F')
    
        # Fill arrays with data
        # GCM current
        n1, n2, n3, n4 = hist.shape
        gcmc[:n1, :n2, :n3, :n4] = hist
    
        # GCM Future
        n1, n2, n3, n4 = fut.shape
        gcmf[:n1, :n2, :n3, :n4] = fut
    
        # Observations current
        n1, n2, n3, n4 = ref.shape
        obsc[:n1, :n2, :n3, :n4] = ref
        # Observations future
        obsf[:n1, :n2, :n3, :n4] = ref
    
    
        ## Parameters for fortran function
        nycur = data_config['nyh']
        nsc = data_config['nsh'] - 1
        nyfut = data_config['nyh']
        nsf = data_config['nsh'] - 1
        ngcur = data_config['nyc']
        nsgc = data_config['nsc'] - 1
        ngfut = data_config['nyf']
        nsgf = data_config['nsf'] - 1
        itime = data_config['itime']
        miss = data_config['miss']
    
        # Dictionary for inputs
        params = {'nycur':  nycur,      'nsc':   nsc,       'nyfut':  nyfut,
                  'nsf':    nsf,        'ngcur': ngcur,     'nsgc':   nsgc,
                  'ngfut':  ngfut,      'nsgf':  nsgf,      'nvar':   nvars,
                  'itime':  itime,      'miss':  miss,      'iband':  iband,
                  'phlwr':  phlwr,      'phupr': phupr,     'igg':    igg,
                  'ilimit': ilimit,     'thres': thres,     'nday':   nday,
                  'idays':  idays,      'nyrmax': NYRMAX,   'nvarmax': NVARMAX,      
                  'gcmc':  gcmc,      'gcmf':   gcmf,       'rec':    obsc,       
                  'ref':   obsf,      'gcmcbc': gcmcbc,     'gcmfbc': gcmfbc}
    
        return params

    def do_bc(ref, hist, fut):
        """Apply MRNBC bias correction to a chunk of data
        """
        
        # Import FMRNBC module
        from module import mrnbc

        # days and mons
        nmon = 13
        nday = 31

        nvars = len(data_config['all_vars'])
        
        # Get parameters for the fortran function
        params = get_params(ref, hist, fut, data_config)
    
        # Run bias correction
        mrnbc(**params)
    
        # Bias corrected GCM current
        gcmcbc = params['gcmcbc']
        gcmcbc = gcmcbc[~np.all(gcmcbc==data_config['fillval'], axis=-1), 3:3+nvars]

        # Bias corrected GCM Future
        gcmfbc = params['gcmfbc']
        gcmfbc = gcmfbc[~np.all(gcmfbc==data_config['fillval'], axis=-1), 3:3+nvars]

        # cut out leap days if appropriate
        if data_config['leapcode'] == 1:
            gcmcbc = gcmcbc[data_config['index_filter']]
            gcmfbc = gcmfbc[data_config['index_filter_fut']]
            
        return gcmcbc, gcmfbc
    
    
    # Apply bias correction for each chunk in the data
    return xr.apply_ufunc(do_bc, ref_arr, hist_arr, fut_arr, 
                          vectorize=True, dask='parallelized',
                          output_dtypes=[np.float32, np.float32],
                          input_core_dims=[["s_vars", "year", "month", "day"], 
                                           ["s_vars", "year", "month", "day"], 
                                           ["s_vars", "year", "month", "day"]],
                          output_core_dims=[["timec", "s_vars"], 
                                            ["timef", "s_vars"]],
                          dask_gufunc_kwargs={"output_sizes": output_sizes})










