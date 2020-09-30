#!/bin/bash

#module load anaconda/2018.12
source activate downscaled_slr
echo which python

scriptdir='/uufs/chpc.utah.edu/common/home/u1070830/code/model-tools/era5'

sy=1980
ey=2020

nlat=30
xlat=30
nlon=-128
xlon=-127.75

dlat=0.25
dlon=0.25

for ilat in $(seq $nlat $dlat $xlat) 
do
    for ilon in $(seq $nlon $dlon $xlon) 
    do
        echo $ilat $ilon
        python $scriptdir/extract_profile.py $ilat $ilon $sy $ey
        python $scriptdir/aggregate_profile.py $ilat $ilon
    done
done

exit