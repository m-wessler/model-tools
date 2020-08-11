import time
import os
import sys
import numpy as np
import xarray as xr
from glob import glob
from functools import partial
from multiprocessing import Pool, cpu_count

def ncks_subset(i, fl, gridtype, wait=True):
    from time import sleep
    from subprocess import call
    from psutil import virtual_memory
    
    f = fl[i]
    mem_need = round(os.path.getsize(f)/10e8, 3) * 1.5
    
    if gridtype == 'isobaric':
        cmd = 'ncks --no_tmp_fl -O -d longitude,928,1040 -d latitude,160,241 -d level,14,36 {0}.nc {0}.WE.nc'.format(f[:-3])

    elif gridtype == 'surface':
        cmd = 'ncks --no_tmp_fl -O -d longitude,928,1040 -d latitude,160,241 {0}.nc {0}.WE.nc'.format(f[:-3])
    
    while wait:
        mem_avail = round(virtual_memory().available/10e8, 3)

        if mem_avail > (mem_need*2):
            print('Processing {}/{} REQ:{}GB AVAIL:{}GB [{}]'.format(i+1, len(fl), mem_need, mem_avail, f))
            wait = False
            return call(cmd, shell=True)
            
        else:
            print('Waiting - RAM Full {}/{} {}GB {}GB [{}]'.format(i+1, len(fl), mem_need, mem_avail, f))
            sleep(15)

if __name__ == '__main__':

    griddtype = sys.argv[1]

    isodir = '/scratch/general/lustre/u1070830/era5_iso/'
    sfcdir = '/scratch/general/lustre/u1070830/era5_sfc/'
    model_dir = isodir if griddtype == 'isobaric' else sfcdir

    dirlist = np.array(glob(model_dir + '*'))
    dirlist = dirlist[np.argsort(dirlist)]

    for d in dirlist:
        print('pwd: %s'%d)
        
        flist = glob(d + '/*.nc')
        flist = [f for f in flist if '.WE.nc' not in f]

        if len(flist) > 0:
            print(len(flist), d)

            # Add a failsafe that loads flist[0] and displays the lat/lon bounds
            # Then yes/no prompt for user to continue with this year and var set
            # ncks_subset_mp = partial(ncks_subset, fl=flist, gridtype=griddtype)

            ncks_subset_mp(0)
            f = glob(d + '/*.WE.nc')[0]
            sample_post = xr.open_dataset(f)
            lon = sample_post.longitude-360
            xlon, nlon = lon.max().values, lon.min().values
            sample_post.close()
            os.remove(f)

            if xlon == -100.0 and nlon == -128.0:
                print(xlon, nlon)
                time.sleep(5)

                p = Pool(cpu_count()-1)
                returns = p.map(ncks_subset_mp, range(len(flist)), chunksize=1)
                p.close()
                p.join()
            
            else:
                print('LAT LON SUBSET INCORRECT')
                raise

            # BE CAREFUL HERE!
            nfiles = 300 if griddtype == 'isobaric' else 60
            flist_check = glob(d + '/*.WE.nc')
            # if len(flist_check) >= nfiles:
            if len(flist_check) == len(flist):
                [os.remove(f) for f in flist]
