#!/bin/bash

#module load anaconda/2018.12
source activate downscaled_slr
echo which python

scriptdir='/uufs/chpc.utah.edu/common/home/u1070830/code/model-tools/era5'
lat=$1
lon=$2
sy=$3
ey=$4

python $scriptdir/extract_profile.py $lat $lon $sy $ey
python $scriptdir/aggregate_profile.py $lat $lon

exit
