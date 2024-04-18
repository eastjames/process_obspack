import glob
import xarray as xr
import numpy as np
import pandas as pd
import yaml
import os
import functools

print = functools.partial(print, flush=True)


def getfiles(config):
    # Get a list of the files
    files = sorted(glob.glob(f'{config["datadir"]}/*.nc'))
    files = [f for f in files if f.split('/')[-1][:11] != 'obspack_ch4']

    # Iterate through the files and see which are relevant to the domain
    myfiles = []
    platforms = []
    print(len(files))
    print('Checking files',flush=True)
    for i, f in enumerate(files):
        op = xr.open_dataset(f)

        # Only use files in the needed time, latitude, and longitude
        # ranges
        try:
            op = filter_obspack(op)
        except ValueError:
            continue
        except KeyError as e:
            print(e, 'skipping: ', f)
            continue

        # If the file is empty, continue through the loop
        if len(op.obs) == 0:
            continue

        # If the file still has observations, append it to myfiles
        myfiles.append(f)

        # And get information on the platform
        platforms.append(op.attrs['dataset_project'])

    # Sort the files
    myfiles.sort()
    return myfiles


# Define a filtering function
def filter_obspack(data):
    # Subset variables
    data_vars = [
        'time', 'start_time', 'midpoint_time', 'time_components', 'value',
        'latitude', 'longitude', 'altitude', 'assimilation_concerns',
        'obspack_id', 'obs_flag', 'qcfilter'
    ]
    try:
        data = data[data_vars]
    except KeyError:
        data_vars.remove('qcfilter')
        data = data[data_vars]
    except KeyError:
        data_vars.remove('obs_flag')
        data = data[data_vars]

    # Subset for time and location
    data = data.where(
        (data['time'] >= config['start_time']) &
        (data['time'] <= config['end_time']+pd.Timedelta('1D')),
        #data['time'].dt.year.isin(keepyears),
        drop=True
    )

    data = data.where(
        (data['latitude'] >= config['lat_min']) & 
        (data['latitude'] <= config['lat_max']),
        drop=True
    )
    
    data = data.where(
        (data['longitude'] >= config['lon_min']) & 
        (data['longitude'] <= config['lon_max']),
        drop=True
    )

    # Save out a platform variable
    platform = data.attrs['dataset_project'].split('-')[0]
    data['platform'] = xr.DataArray(
        [platform]*len(data.obs), 
        dims=('obs')
    )

    if 'qcflag' not in data.data_vars:
        data['qcflag'] = xr.DataArray(
            np.full(len(data.obs),'...',dtype='S10'),
            dims=('obs')
        )

    if 'obs_flag' not in data.data_vars:
        data['obs_flag'] = xr.DataArray(
            np.full(len(data.obs),1.,dtype=np.float64),
            dims=('obs')
        )

    # Correct to local timezone if it's an in situ or surface observation
    if (len(data.obs) > 0) and (platform in ['surface', 'tower']):
        utc_conv = data.attrs['site_utc2lst']
        if int(utc_conv) != utc_conv:
            errstr = (
                'UTC CONVERSION FACTOR IS NOT AN '
                f'INTEGER : {data.attrs["dataset_name"]}'
            )
            print(errstr)
        data['utc_conv'] = xr.DataArray(
            utc_conv*np.ones(len(data.obs)),
            dims=('obs')
        )
        
    else:
        data['utc_conv'] = xr.DataArray(
            np.zeros(len(data.obs)),
            dims=('obs')
        )

    return data





def open_all_files(myfiles, config):
    # Now load all the files
    print('Opening all files',flush=True)
    ds = xr.open_mfdataset(
        myfiles, concat_dim='obs', combine='nested', 
        chunks=int(1e4), mask_and_scale=False, 
        preprocess=filter_obspack
    )

    # Check for the sampling strategy
    ## Get the time in hours of each sample
    ds['obs_length'] = (ds['midpoint_time'] - ds['start_time'])
    ds['obs_length'] = ds['obs_length'].dt.seconds*2/(60*60)

    ## Convert that to the sampling strategy flag
    ## ss = place holder for sampling strategy
    ds['ss'] = xr.DataArray(999*np.ones(len(ds.obs)), dims=('obs'))

    ## Closest to 4 hours
    ds['ss'] = ds['ss'].where(ds['obs_length'] > 5.25, 1)

    ## Closest to 90 minutes
    ds['ss'] = ds['ss'].where(ds['obs_length'] > 2.75, 3)

    ## Closest to 1 hour
    ds['ss'] = ds['ss'].where(ds['obs_length'] > 1.25, 2)

    ## Closest to instantaneous
    ds['ss'] = ds['ss'].where(ds['obs_length'] > 0.5, 4)

    ## Cast to int
    ds['ss'] = ds['ss'].astype(int)

    # Rename and add attributes
    ds = ds.rename({'ss' : 'CT_sampling_strategy'})
    ds['CT_sampling_strategy'].attrs = {
        '_FillValue' : -9,
        'long_name' : 'model sampling strategy',
        'values' : 'How to sample model. 1=4-hour avg; 2=1-hour avg; 3=90-min avg; 4=instantaneous'
    }

    # Other clean up
    ds.attrs = {}
    ds = ds.drop(['obs_length', 'start_time', 'midpoint_time'])
    return ds


def saveday(ds, mydate, config):
    daily = ds.where(
        (
            (ds['time'] >= mydate) & 
            (ds['time'] < mydate+pd.Timedelta('1D')) &
            (ds['CT_sampling_strategy'].isin([1,2,3,4]))
        ),
        drop=True
    )
    # If there is no data, continue
    if len(daily.obs) == 0:
        return
    
    # Data type fix
    daily['obspack_id'] = daily['obspack_id'].astype('S200')
    daily['platform'] = daily['platform'].astype('S50')
    daily['qcflag'] = daily['qcflag'].astype('S10')
    
    # Time fix
    tunits = 'seconds since 1970-01-01 00:00:00 UTC'

    daily['time_components'].attrs['_FillValue'] = float(-9.)
    daily['value'].attrs['_FillValue'] = float(-1e34)
    daily['latitude'].attrs['_FillValue'] = float(-1e34)
    daily['longitude'].attrs['_FillValue'] = float(-1e34)
    daily['altitude'].attrs['_FillValue'] = float(-1e34)
    daily['CT_sampling_strategy'].attrs['_FillValue'] = float(-9.)
    
    # Otherwise, save out
    print(f'Saving {mydate.strftime("%Y-%m-%d")}',flush=True)
    outpath=f'{config["outdir"]}/{config["outfile_name_stem"]}'
    daily.to_netcdf(
        mydate.strftime(outpath),
        unlimited_dims=['obs'],
        encoding = {
            **{
                v:{'complevel':1,'zlib':True}
                for v in daily.data_vars
            },
            'time':{'units':tunits,'calendar':'proleptic_gregorian'}
        }
    )

    return


if __name__ == '__main__':
    # load config opts
    with open('config.yml','r') as configfile:
        config = yaml.safe_load(configfile)

    config['start_time'] = pd.to_datetime(
        config['start_time'], format='%Y-%m-%d'
    )
    config['end_time'] = pd.to_datetime(
        config['end_time'], format='%Y-%m-%d'
    )
    
    os.makedirs(config['outdir'], exist_ok=True)

    myfiles = getfiles(config)
    ds = open_all_files(myfiles, config)

    mydates = pd.date_range(config['start_time'],config['end_time'],freq='D')
    for dd in mydates:
        saveday(ds, dd, config)
