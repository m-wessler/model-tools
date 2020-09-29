import os
import gc
import sys
import numpy as np
from glob import glob
import xarray as xr

from subprocess import run, PIPE

from multiprocessing import get_context, Pool, cpu_count
from functools import partial

os.environ['OMP_NUM_THREADS'] = '1'

isodir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/iso/'
sfcdir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/sfc/'
profdir = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/era5/profiles/disagg/'

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

def get_year(year, key, levelset, xi, yi):
    
    print('Working on: %s %04d'%(key, year))
        
    year_data = []
    for month in np.arange(1, 12+1):
        
        datadir = isodir if levelset == 'iso' else sfcdir
        date_dir = datadir + '%04d%02d'%(year, month)

        flist = sorted(glob(date_dir + '/*_%s.*'%key))
        
        try:
            month_data = xr.open_mfdataset(flist, concat_dim='time', drop_variables=['utc_date'], parallel=True,
                                      decode_cf=True, decode_times=True, decode_coords=False,
                                     ).isel(latitude=yi, longitude=xi).drop(['latitude', 'longitude'])
        except:
            print('Failed: %04d %02d'%(year, month))
        
        else:            
            if levelset == 'iso':
                month_data = month_data.chunk({'time':month_data[key.upper()].shape[0]*1, 
                                 'level':month_data[key.upper()].shape[1]*1}).load()
            else:
                try:
                    int(key[0])
                except:
                    month_data = month_data.chunk({'time':month_data[key.upper()].shape[0]*1,}).load()
                else:
                    month_data = month_data.chunk({'time':month_data['VAR_' + key.upper()].shape[0]*1,}).load()
                    
            month_data.attrs = {}
            year_data.append(month_data)

    try:
        year_data = xr.concat(year_data, dim='time')
        year_data = year_data#.chunk({'time':year_data[key.upper()].shape[0],
                             #   'level':year_data[key.upper()].shape[1]})
    except:
        return None
    else:
        return year_data#.load()
    
if __name__ == '__main__':

    lat, lon, start, end = 40.50, -111.50, 1980, 2020 #sys.argv[1:]
    lat, lon = float(lat), float(lon)
    start, end = int(start), int(end)

    print('Creating ERA5 Profile: {}, {}, {}, {}'.format(
        lat, lon, start, end))

    isokeys = ['q', 't', 'u', 'v', 'vo', 'w', 'z', 'r']
    sfckeys = ['100u', '100v', '10u', '10v', '2d', '2t', 'blh', 'cape', 'msl', 'sp']

    sample = xr.open_dataset('./era5_iso_sample.nc')
    a = abs(sample['latitude']-lat)+abs(sample['longitude']-360-lon)
    yi, xi = np.unravel_index(a.argmin(), a.shape)

    lat = sample.isel(latitude=yi, longitude=xi)['latitude']
    lon = sample.isel(latitude=yi, longitude=xi)['longitude'] - 360
    print('ERA5 profile at gridpoint: %.2f, %.2f'%(lat, lon))
    
    for key in np.append(isokeys, sfckeys):
        
        filepath = mkdir_p(profdir) + 'era5prof_{:.2f}N_{:.2f}W.{:s}.{:04d}_{:04d}.nc'.format(
            lat.values, abs(lon.values), key.upper(), start, end)
    
        if os.path.isfile(filepath):
            print(filepath, 'exists, skipping')
            pass
        else:
            levelset = 'iso' if key in isokeys else 'sfc'
            mpfunc = partial(get_year, key=key, levelset=levelset, xi=xi, yi=yi)

            with get_context('forkserver').Pool(len(np.arange(start, end+1))) as p: #start, end+1)))
                result = p.map(mpfunc, np.arange(start, end+1), chunksize=1)
                p.close()
                p.join()

            result = [r for r in result if r is not None]
            result = xr.concat(result, dim='time')

            result.to_netcdf(filepath, engine='h5netcdf')
            print('Saved: ', filepath)

            del result
            gc.collect()

    

