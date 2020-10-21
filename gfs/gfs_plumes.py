import gc, os, sys
import pickle
import cfgrib
import pygrib

import pandas as pd
import numpy as np
import xarray as xr

import matplotlib.pyplot as plt

from glob import glob
from functools import reduce
from datetime import datetime
from sklearn.preprocessing import RobustScaler
from multiprocessing import get_context

os.environ['OMP_NUM_THREADS'] = '1'
mp_use_cores = 32

model = 'gfs0p25'
archive = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/archive/'
output = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/gfs/'

date_fmt = '%Y%m%d'
datetime_fmt = '%Y%m%d%H'

def ingest_gfs(f):
    
    # print('Reading %s'%os.path.basename(f))

    datasets = cfgrib.open_datasets(f)

    keep_keys = ['tp', 'q', 't', 'u', 'v', 'absv', 'w', 'gh', 'r', 'd', 
                  'u10', 'v10', 'u100', 'v100', 't2m', 'd2m', 
                  'cape', 'prmsl', 'sp', 'orog', 'hpbl']

    sfc, iso = [], []

    for ds in datasets:

        key_match = np.array(list(ds.data_vars))[np.isin(list(ds.data_vars), keep_keys)]

        if len(key_match) > 0:

            dims = ds.dims.keys()
            coords = ds[key_match].coords

            if ('heightAboveGround' in coords) & ('heightAboveGround' not in dims):
                sfc.append(ds[key_match].drop('heightAboveGround'))

            elif 'isobaricInhPa' in coords:
                iso.append(ds[key_match])

            elif (('surface' in coords)|('meanSea' in coords)):
                sfc.append(ds[key_match])

            elif 'prmsl' in list(ds.data_vars):
                sfc.append(ds['prmsl'])

            else:
                pass

        else:
            pass
        
        ds.close()

    sfc = xr.merge(sfc).drop('t')
    iso = xr.merge(iso).rename({'isobaricInhPa':'level'})
    iso = iso.sel(level=iso.level[::-1])

    sfc['longitude'] = sfc['longitude'] - 360
    iso['longitude'] = iso['longitude'] - 360
    
    return [sfc, iso]

if __name__ == '__main__':

    init = datetime.strptime(sys.argv[1], '%Y%m%d%H')
    
    flist = glob(archive + init.strftime(date_fmt) + '/models/%s/*%s*.grib2'%(
        model, init.strftime(datetime_fmt)))[1:]

    if len(flist) > 0:
    
        workers = len(flist) if len(flist) <= mp_use_cores else mp_use_cores
        with get_context('fork').Pool(workers) as p:
            returns = p.map(ingest_gfs, flist, chunksize=1)
            p.close()
            p.join()
            
        print('Processing data for %s'%init.strftime(datetime_fmt))

        returns = np.array(returns, dtype=object)
        sfc, iso = returns[:, 0], returns[:, 1]

        iso = xr.concat(iso, dim='valid_time').drop('time').rename({'valid_time':'time'}).sortby('time')
        sfc = xr.concat(sfc, dim='valid_time').drop('time').rename({'valid_time':'time'}).sortby('time')

        u, v = iso['u'], iso['v']
        wdir = 90 - np.degrees(np.arctan2(-v, -u))
        wdir = xr.where(wdir <= 0, wdir+360, wdir)
        wdir = xr.where(((u == 0) & (v == 0)), 0, wdir)

        iso['dir'] = wdir
        iso['spd'] = np.sqrt(u**2 + v**2)

        for hgt in [10, 100]:

            u, v = sfc['u%d'%hgt], sfc['v%d'%hgt]
            wdir = 90 - np.degrees(np.arctan2(-v, -u))
            wdir = xr.where(wdir <= 0, wdir+360, wdir)
            wdir = xr.where(((u == 0) & (v == 0)), 0, wdir)

            sfc['dir%dm'%hgt] = wdir
            sfc['spd%dm'%hgt] = np.sqrt(u**2 + v**2)

        orog = sfc.orog
        gh = iso.gh

        lowest_level = np.full(orog.shape, fill_value=np.nan)
        lowest_level_index = np.full(orog.shape, fill_value=np.nan)

        for i, level in enumerate(iso['level']):

            lev_gh = gh.sel(level=level)
            lowest_level = xr.where(orog >= lev_gh, level.values, lowest_level)
            lowest_level_index = xr.where(orog >= lev_gh, i, lowest_level_index)

        lowest_level_index = xr.where(np.isnan(lowest_level), 0, lowest_level_index)
        lowest_level = xr.where(np.isnan(lowest_level), 1000, lowest_level)

        df = []
        match_rename = {'absv':'vo', 'gh':'z', 'hpbl':'blh', 'prmsl':'msl', 'tp':'swe_mm',
                       'u10':'u10m', 'v10':'v10m', 'u100':'u100m', 'v100':'v100m'}

        # Loop over each variable in the xarray
        for ds in [iso, sfc.drop('orog')]:

            for var_name in ds.data_vars:

                new_var_name = match_rename[var_name] if var_name in match_rename.keys() else var_name
                #print('Reducing (%s) to %s index level AGL'%(var_name, new_var_name))

                var = ds[var_name]

                if 'level' in var.coords:

                    for i in np.arange(10):

                        var_agl = np.full(shape=(orog.shape), fill_value=np.nan)

                        for j, level in enumerate(iso['level']):

                            var_agl = xr.where(lowest_level_index+i == j, var.isel(level=j), var_agl)

                            # Record the levels used, should match lowest_level array, sanity check
                            # var_agl[i, :, :] = xr.where(lowest_level_index+i == j, level, var_agl[i, :, :])

                        # We could go ahead and append to the pandas dataframe here 
                        # at the completion of each level (_01agl, _02agl...)
                        # We will have to use [(time), lat, lon] as a multiindex
                        var_agl = xr.DataArray(var_agl, 
                             dims=['time', 'latitude', 'longitude'], 
                             coords={'time':ds['time'],
                                     'latitude':ds['latitude'], 
                                     'longitude':ds['longitude']})

                        df.append(var_agl.to_dataframe(name='%s_%02dagl'%(new_var_name.upper(), i+1)))

                        del var_agl
                        gc.collect()

                else:

                    var_agl = xr.DataArray(var.values, 
                        dims=['time', 'latitude', 'longitude'], 
                        coords={'time':ds['time'],
                            'latitude':ds['latitude'], 
                             'longitude':ds['longitude']})

                    df.append(var_agl.to_dataframe(name='%s'%new_var_name.upper()))

        # SLOW!!! Is there anything we can do here??
        print('Merging Dataframe')
        df = reduce(lambda left, right: pd.merge(left, right, on=['time', 'latitude', 'longitude']), df)
        df = df.rename(columns={'SWE_MM':'swe_mm'})

        print('Saving Dataset %s.nc'%init.strftime(datetime_fmt))
        ds = df.to_xarray()
        ds = ds.assign_coords({'init':init})
        ds.to_netcdf(output + init.strftime(datetime_fmt) + '.nc')
        print('%s.nc saved'%init.strftime(datetime_fmt))
        
    else:
        print('No files found for %s'%init)