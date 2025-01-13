# MRNBC

A python wrapper for the Multivariate Recursive Nested Bias Correction (MRNBC) Fortran package. 

Generic code is located in pymrnbc.py, however the other scripts (particularly example_calc_mrnbc) show how it is used and include important parameters that the code expects.

## Running the example scripts

Requirements: Running the bias correction example requires access to following NCI projects: hh5, dk92, ia39

1. Compile the Fortran code by running the following command in the directory:
   - `bash pybuild.sh`

2. Change the temp and output directories in the 5 example Python scripts (example_calc_mrnbc, example_chunk_mdl, example_chunk_ref, example_convert_to_nc and example_repair_mrnbc) to relevant and accessible directories for your use. You may need to check the job pbs scripts for relevant dependencies depending on which projects your chosen directories are in. Home directories are not advised to be chosen here due to space requirements.

3. Run the reference data chunking script:
   - `qsub job-ref-chunk.pbs`
     
    The output log will appear in the logs folder.

4. After job-ref-chunk has completed, select which steps to complete in the all_jobs.sh shell file.
   
   Modifying start_from and finish_at (i.e. not completing all 4 steps) is only recommended for testing purposes, error correction or to handle large workloads dynamically.

5. Submit the jobs:
   - `bash all_jobs.sh`
  
   The output logs will appear nested in the logs folder.

## What the submitted jobs are

The 4 steps submitted by the all_jobs script are as follows:
1. Data chunking - this will load the required variables from the given model data, rechunk and save into Zarr format so it may be more easily processed later.
2. MRNBC calculation - this takes the chunked model data (and chunked reference data from job-ref-chunk) and applies the MRNBC bias correction to it. The corrected data is again saved into Zarr format.
3. MRNBC repair - this is a workaround step for an issue that was discovered in the previous step that has so far been unresolved. It checks the output from the previous step and reruns the MRNBC on locations where the errors occurred. This process can be very quick or very slow depending on how prevalent the error was.
4. Conversion - this will take the output Zarr files and convert them into yearly NetCDF files. Relevant metadata is taken from the original model data and appropriately adjusted - many of the changes can be found in the .yml files in the project.

The intermediate data requirements for running MRNBC can be quite large - ensure there is sufficient space and inodes available in the temporary directory of your choice. Each MRNBC output Zarr file is deleted upon being fully converted to NetCDF in an attempt to save space, but the chunked input data is not.

## Tracking jobs

When running multiple jobs simultaneously, the client_cmd file created in the home directory by Gadi to help track the job will be rapidly overwritten. Thankfully, a copy for each job will exist in their hidden configuration folders (`.cfg_{job-id}.gadi-pbs/client_cmd`).

## Why is the repair script necessary

Unfortunately during the MRNBC calculation it was found that the output of certain random areas of the first variable (and sometimes the last variable when run with a large number of variables, in this case 7) were all set to their maximum value. The cause of this problem has not yet been located, so a workaround has been implemented for now (to detect and rerun the algorithm on those areas until they have sensible values).

## TODO

A lot of parameters in the example scripts need to be manually adjusted to match each other (such as year ranges, directory locations etc.) and may fail if one of them is forgotten. There is certainly room for improvement or simplification in this regard, though end users are free to take the example scripts as a base and implement this themselves.

