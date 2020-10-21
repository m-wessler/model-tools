import os
import shlex
import numpy as np
from pandas import to_datetime, date_range
from datetime import datetime
from multiprocessing import get_context
from subprocess import call

os.environ['OMP_NUM_THREADS'] = '1'

python = '/uufs/chpc.utah.edu/common/home/u1070830/anaconda3/envs/xlab/bin/python '
plume_script = '/uufs/chpc.utah.edu/common/home/u1070830/code/snow-liquid-ratio/core/gfs_plumes.py '

def launcher(date):
    
    cmd = python + plume_script + to_datetime(date).strftime('%Y%m%d%H')
    call(shlex.split(cmd))
    
    return None

if __name__ == '__main__':
    
    date_list = np.hstack([
        date_range(
            datetime(2015, 1, 15, 0, 0), 
            datetime(2015, 5, 31, 23, 0), 
            freq='12H'),
        date_range(
            datetime(2016, 10, 1, 0, 0), 
            datetime(2017, 5, 31, 23, 0), 
            freq='12H'),
        date_range(
            datetime(2017, 10, 1, 0, 0), 
            datetime(2018, 5, 31, 23, 0), 
            freq='12H'),
        date_range(
            datetime(2018, 10, 1, 0, 0), 
            datetime(2019, 5, 31, 23, 0), 
            freq='12H'),
        date_range(
            datetime(2019, 10, 1, 0, 0), gm
            datetime(2020, 5, 31, 23, 0), 
            freq='12H'),])
    
    print(date_list)
  
    [launcher(d) for d in date_list]

#     with get_context('spawn').Pool(15) as p:
#         p.map(launcher, date_list, chunksize=1)
#         p.close()
#         p.join()