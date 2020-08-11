import os
import gc
import sys
import numpy as np
from glob import glob
import xarray as xr

from subprocess import run, PIPE

from multiprocessing import Pool, cpu_count
from functools import partial

# Config
# site = 'CLN'
# lat, lon, elev = 40.5763, -111.6383, 2945
#start, end = 1999, 2019

lat, lon, start, end = sys.argv[1:]
lat, lon = float(lat), float(lon)
start, end = int(start), int(end)

print('Creating ERA5 Profile: {}, {}, {}, {}'.format(
    lat, lon, start, end))

def mkdir_p(path):
    import errno    
    import os

    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
    
    return path

def load_year(y, v, lev):
    
    print('working', y, v)
    init = True
        
    flist = []
    ddir = isodir if lev == 'iso' else sfcdir
    dirs = np.array(glob(ddir + '*%s*'%y))
    dirs = dirs[np.argsort(dirs)]

    for mdir in dirs:
        flist.extend(glob(mdir + '/*_%s.*.nc'%v))

    flist = np.array(flist)
    flist = flist[np.argsort(flist)]

    if init:
        ds = xr.open_dataset(flist[0])
        ds['longitude'] -= 360
        a = abs(ds.latitude-lat)+abs(ds.longitude-lon)
        yi, xi = np.unravel_index(a.argmin(), a.shape)

        del ds
        init = False

    if lev == 'sfc':
        try:
            int(v[0])
        except:
            v = v
        else:
            v = 'var_' + v
    try:
        ds = xr.open_mfdataset(flist, combine='nested', concat_dim='time')[v.upper()].isel(longitude=xi, latitude=yi).load()  
    except:
        ds = xr.open_mfdataset(flist, concat_dim='time')[v.upper()].isel(longitude=xi, latitude=yi).load()  
    print(y, v, 'loaded')
    
    return ds

if __name__ == '__main__':

    isodir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/iso/'
    sfcdir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/sfc/'
    profdir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/profiles/disagg/'
    
    # ['cc', 'ciwc', 'clwc', 'crwc', 'cswc', 'd', 'o3', 'pv'] 
    # isokeys = ['q', 't', 'u', 'v', 'vo', 'w', 'z', 'r']
    
    # ['alnid', 'alnip', 'aluvd', 'aluvp', 'asn', 'chnk', 'ci',
    # 'fal', 'flsr', 'fsr', 'hcc', 'ie', 'iews', 'inss', 'ishf', 
    # 'istl1', 'istl2', 'istl3', 'istl4', 'laihv', 'lailv', 'lblt', 
    # 'lcc', 'licd', 'lict', 'lshf', 'ltlt', 'mcc', 'rsn', 'sd', 'skt', 
    # 'src', 'sstk', 'stl1', 'stl2', 'stl3', 'stl4', 'swvl1', 'swvl2', 
    # 'swvl3', 'swvl4', 'tcc', 'tciw', 'tclw', 'tco3', 'tcrw', 'tcsw', 
    # 'tcw', 'tcwv', 'tsn', 'u10n', 'v10n']
    #sfckeys = ['100u', '100v', '10u', '10v', '2d', '2t', 'blh', 'cape', 'msl', 'sp']

    isokeys = ['q', 't', 'u', 'v', 'vo', 'w', 'z', 'r']
    sfckeys = ['100u', '100v', '10u', '10v', '2d', '2t', 'blh', 'cape', 'msl', 'sp']
    
    for k in isokeys:

        year_list = np.arange(start, end+1)
        load_year_mp = partial(load_year, v=k, lev='iso')
        
        p = Pool(len(year_list))
        _data = p.map(load_year_mp, year_list, chunksize=1)
        p.close()
        p.join()

        data = xr.concat(_data, dim='time')
        del _data; gc.collect()
        
        filepath = mkdir_p(profdir) + 'era5prof_{}N_{}W.{}.{}_{}.nc'.format(lat, abs(lon), k.upper(), start, end)
      
        data.to_netcdf(filepath)
        del data; gc.collect()

    for k in sfckeys:
    
        year_list = np.arange(start, end+1)
        load_year_mp = partial(load_year, v=k, lev='sfc')
        
        p = Pool(len(year_list))
        _data = p.map(load_year_mp, year_list, chunksize=1)
        p.close()
        p.join()

        data = xr.concat(_data, dim='time')
        del _data; gc.collect()
        
        filepath = mkdir_p(profdir) + 'era5prof_{}N_{}W.{}.{}_{}.nc'.format(lat, abs(lon), k.upper(), start, end)

        data.to_netcdf(filepath)
        del data; gc.collect()
