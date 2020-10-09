#!/bin/bash
conda activate xlab
alias python='/uufs/chpc.utah.edu/common/home/u1070830/anaconda3/envs/xlab/bin/python'

sy=1980
ey=2020

nlat=30
xlat=50
nlon=-128
xlon=-100

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