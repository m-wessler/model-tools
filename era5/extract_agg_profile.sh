#!/bin/bash
conda activate xlab
alias python='/uufs/chpc.utah.edu/common/home/u1070830/anaconda3/envs/xlab/bin/python'

scriptdir='/uufs/chpc.utah.edu/common/home/u1070830/code/model-tools/era5'
lat=$1
lon=$2
sy=$3
ey=$4

echo $python

python $scriptdir/extract_profile.py $lat $lon $sy $ey
python $scriptdir/aggregate_profile.py $lat $lon

exit
