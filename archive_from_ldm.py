import os, sys
import shlex
import numpy as np
import pandas as pd

from glob import glob
from subprocess import Popen, call, PIPE
from datetime import datetime, timedelta

from functools import partial
from multiprocessing import get_context, Pool, cpu_count

os.environ['OMP_NUM_THREADS'] = '1'

archive_models = ['naefs']
archive_gribs = ['naefs'] #'gfsds', 'namds', 'sref'
archive_output = [] #'naefs', 'gfsds', 'namds', 'sref'

archive_hours = {'naefs':[0, 12], 'sref':[3, 9, 15, 21],
                 'gfsds':[0, 6, 12, 18], 'namds':[0, 6, 12, 18]}

grib_ext = {'naefs':'grib2'}

horel_archive = '/uufs/chpc.utah.edu/common/home/horel-group/archive/'
local_archive = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/archive/'

# Later set up to run on 16th/1st of the month on cron!
#start = datetime.now() - timedelta(days=22)
#start = datetime(start.year, start.month, start.day, 0, 0)
start = datetime(2021, 1, 13, 18, 0)

#end = datetime.now() - timedelta(days=1)
#end = datetime(end.year, end.month, end.day, 0, 0)
end = datetime(2021, 1, 27, 0, 0)

def tar_archive(init, model):
    
    grib_format = {'naefs':'%%Y%%m%%d%%H*.%s'%grib_ext[model]}
    
    init_str = datetime.strftime(init, '%Y%m%d%H')
    init_hour = init.hour
    init_date_str = datetime.strftime(init, '%Y%m%d')

    orig_dir = '%s%s/models/%s/'%(horel_archive, init_date_str, model)
    dest_dir = '%s%s/models/%s/'%(local_archive, init_date_str, model)
    os.makedirs(dest_dir, exist_ok=True)
    
    if init_hour in archive_hours[model]:

        if model in archive_gribs:

            orig_files = orig_dir + '*%s*.%s'%(init_str, grib_ext[model])                        
            dest_tarfile = dest_dir + '%s.%s.tar.gz'%(init_str, grib_ext[model])

            if not os.path.isfile(dest_tarfile):
                tar_cmd = 'cd %s && tar -czvf %s %s'%('/'.join(orig_files.split('/')[:-1]), dest_tarfile, orig_files.split('/')[-1])
                #print(tar_cmd)
                #call(shlex.split(tar_cmd))
                os.system(tar_cmd)
                print('%s written'%dest_tarfile.split('/')[-1])
                
            else:
                print('%s exists, skipping'%dest_tarfile.split('/')[-1])

        if model in archive_output:

            dest_dir_nest = dest_dir + '%s/'%init_str
            os.makedirs(dest_dir_nest, exist_ok=True)

            orig_files = orig_dir + '%s'%(init_str)
            dest_tarfile = dest_dir_nest + '%s.output.tar.gz'%init_str

            if not os.path.isfile(dest_tarfile):
                
                # It might be worth adding a ncks compression here for the netcdf files first...
                tar_cmd = 'cd %s && tar -czvf %s %s'%('/'.join(orig_files.split('/')[:-1]), dest_tarfile, orig_files.split('/')[-1])
                #print(tar_cmd)
                call(shlex.split(tar_cmd))
                os.system(tar_cmd)
                print('%s written'%dest_tarfile.split('/')[-1])
                
            else:
                print('%s exists, skipping'%dest_tarfile.split('/')[-1])

if __name__ == '__main__':
    
    for model in archive_models:
        
        # Need a fix for the offset SREF (6H start at 03Z)
        inits = pd.date_range(start, end, freq='6H')

        print('Archiving %s'%model)
        print('Start: %s\nEnd: %s'%(start, end))
        print('Hours: %s\n'%archive_hours[model])

        tar_archive_mp = partial(tar_archive, model=model)
        for init in inits:
            tar_archive_mp(init)
        
#         with get_context('spawn').Pool(cpu_count()) as p:    
#             p.map(tar_archive_mp, inits, chunksize=1)
#             p.close()
#             p.join()
