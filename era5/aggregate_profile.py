import numpy as np
import xarray as xr
import pandas as pd

from sys import argv
from glob import glob

scriptdir = '/uufs/chpc.utah.edu/common/home/u1070830/code/model-tools/era5/'
profdir = '/uufs/chpc.utah.edu/common/home/u1070830/mewessler/era5/profiles/'

lat, lon = argv[1:]
lat, lon = float(lat), float(lon)

flist = glob(profdir + 'disagg/' '*%s*%s*.nc'%(lat, abs(lon)))

iso, sfc = [], []
for f in flist:
    d = xr.open_dataset(f)
    if 'level' in list(d.dims):
        iso.append(d.to_dataframe(#).drop(columns=['latitude', 'longitude']
                    ).reset_index().set_index(['time', 'level']).sort_index())
    else:
        sfc.append(d.to_dataframe())#.drop(columns=['latitude', 'longitude']))

isomerge = None
for i in range(1, len(iso)):
    print('Merging isobaric variable %d/%d'%(i, len(iso)-1))
    
    if isomerge is not None:
        isomerge = isomerge.merge(iso[i], on=['time', 'level'])
    else:
        isomerge = iso[i-1].merge(iso[i], on=['time', 'level'])
isomerge = isomerge[~isomerge.index.duplicated()]

sfcmerge = None
for i in range(1, len(sfc)):
    print('Merging surface variable %d/%d'%(i, len(sfc)-1))
    
    if sfcmerge is not None:
        sfcmerge = sfcmerge.merge(sfc[i], on=['time'])
    else:
        sfcmerge = sfc[i-1].merge(sfc[i], on=['time'])  

sfcmerge = sfcmerge[~sfcmerge.index.duplicated()]

sfcmerge = sfcmerge.rename(
    columns={k:k[-1] + k.replace('VAR_', '')[:-1] + 'M' 
             for k in sfcmerge.keys() if 'VAR_' in k})

merge = xr.merge([isomerge.to_xarray(), sfcmerge.to_xarray()])

savestr = 'era5prof_%.2fN_%.2fW.nc'%(lat, abs(lon))
print('Saving: %s'%savestr)

merge.to_netcdf(profdir + savestr)
