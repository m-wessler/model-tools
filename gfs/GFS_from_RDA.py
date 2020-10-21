#!/uufs/chpc.utah.edu/common/home/u1070830/anaconda3/envs/xlab/bin/python

import sys, os
import shlex
import requests
import numpy as np
import pandas as pd
from subprocess import Popen, call, PIPE
from datetime import datetime, timedelta
from multiprocessing import Pool, get_context, cpu_count

os.environ['OMP_NUM_THREADS'] = '1'

start, end, freq = datetime(2017, 11, 15, 0, 0), datetime(2020, 5, 31, 23, 59), '12H'
forecast_hours = np.arange(0, 180+1, 3)
nlon, xlon, nlat, xlat = -130, -100, 30, 50

url = 'https://rda.ucar.edu/cgi-bin/login'
dspath = 'https://rda.ucar.edu/data/ds084.1/'

archive = '/uufs/chpc.utah.edu/common/home/steenburgh-group10/mewessler/archive/'
temp = '/scratch/general/lustre/u1070830/gfs_temp/'

wgrib2 = '/uufs/chpc.utah.edu/sys/installdir/wgrib2/2.0.8/wgrib2/wgrib2'

# Try to get password
if len(sys.argv) < 2 and not 'RDAPSWD' in os.environ:
    try:
        import getpass
        input = getpass.getpass
    except:
        try:
            input = raw_input
        except:
            pass
    pswd = input('Password: ')
else:
    try:
        pswd = sys.argv[1]
    except:
        pswd = os.environ['RDAPSWD']

os.environ['RDAPSWD'] = pswd

# Authenticate
auth_values = {'email' : 'm.wessler@utah.edu', 
            'passwd' : pswd, 'action' : 'login'}
ret = requests.post(url, data=auth_values)
if ret.status_code != 200:
    print('Bad Authentication')
    print(ret.text)
    exit(1)

def check_file_status(filepath, filesize):
    
    sys.stdout.write('\r')
    sys.stdout.flush()
    size = int(os.stat(filepath).st_size)
    percent_complete = (size/filesize)*100
    sys.stdout.write('%.3f %s' % (percent_complete, '% Completed'))
    sys.stdout.flush()
    
def download_file(flist):
    
    for file in flist:
        
        try:
        
            filename = dspath + file
            file_base = os.path.basename(file)
            init_str = file_base.split('.')[2]

            temp_dir = '%s%s/'%(temp, init_str)
            os.makedirs(temp_dir, exist_ok=True)

            subset_dir = '%s%s/models/gfs0p25/'%(archive, init_str[:-2])
            os.makedirs(subset_dir, exist_ok=True)
            subset_file = subset_dir + file_base
            subset_file = subset_file.replace('.grib2', '.WE.grib2')

            if not os.path.isfile(subset_file):

                temp_file = temp_dir + file_base
                print('Downloading', file_base)

                req = requests.get(filename, cookies=ret.cookies, allow_redirects=True, stream=True)
                filesize = int(req.headers['Content-length'])

                if not os.path.isfile(temp_file):

                    with open(temp_file, 'wb') as outfile:
                        chunk_size = 1048576
                        for chunk in req.iter_content(chunk_size=chunk_size):
                            outfile.write(chunk)
                else:
                    print('%s found, skipping'%os.path.basename(temp_file))

                # Generate subset file using wgrib2
                print('Subsetting %s'%os.path.basename(subset_file))
                run_cmd = '%s %s -small_grib %d:%d %d:%d %s > tmp.txt'%(
                    wgrib2, temp_file, nlon, xlon, nlat, xlat, subset_file)
                os.system(run_cmd)
                os.remove(temp_file)
        except:
            pass

        else:
            print('%s subset found, skipping'%os.path.basename(subset_file))
    
    try:
        os.rmdir(temp_dir)
    except:
        pass

    return None

if __name__ == '__main__':

    filelist = []
    init_dates = pd.date_range(start, end, freq=freq)
    
    init_dates = [init for init in init_dates 
                  if init.month in
                  [1, 2, 3, 4, 5, 10, 11, 12]]

    for init in init_dates:
            filelist.append([('{0:04d}/{0:04d}{1:02d}{2:02d}/' + 
                'gfs.0p25.{0:04d}{1:02d}{2:02d}{3:02d}.' + 
                'f{4:03d}.grib2').format(
                init.year, init.month, init.day, init.hour, fhr)
                        for fhr in forecast_hours])
            
    #workers = int(cpu_count()/2)
    workers = 10
    print('Downloading via %d processes'%workers)

    with get_context('fork').Pool(workers) as p:
        p.map(download_file, filelist, chunksize=1)
        p.close()
        p.join()
