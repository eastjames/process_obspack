# Process Obspack CH4 for GEOS-Chem
Author: James East with contributions from Harvard ACMG

## What does it do?
Processes Obspack CH4 output datasets into daily netcdf files to be read by GEOS-Chem's obspack capability. Daily files produced with this program differ from the "daily" files directly available from Obspack. Specifically, they include extra variables including the sampling strategy, the quality flag for each observation, and assimilation concerns.

## How to use
1. Obtain data
    1. request data by filling out form at `https://gml.noaa.gov/ccgg/obspack/data.php`
    2. receive email with data link
    3. choose "NetCDF output datasets in a zip file" and download locally
    4. edit `get.sh` by assigning URL to `data_link` variable, e.g. `data_link=<url_to_output_datasets_zip_file>`
    5. download data: `./get.sh`
    
2. Customize options by editing `config.yml` file
    * `datadir` is where raw data is located, shouldn't need to be changed unless your obspack version is not CH4 GLOBALVIEWplus_v6.0_2023-12-01
    * `outdir` is where processed data will be stored
    * `outfile_name_stem` is the output file name pattern with date components interpretable by `strftime`
    
3. Process data
    * Activate python environment or add lines to `run.job` to activate python env
    * in user shell (not recommended): `python process_obspack.py`
    * using SLURM: `sbatch ./run.job`
        * NOTE: run.job contains recommended SLURM options for Harvard FASRC
        
4. Use in GEOS-Chem
    * use data by adding path to processed data to `geoschem_config` file under the Obspack section and activating Obspack
