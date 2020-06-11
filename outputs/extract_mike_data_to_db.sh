#!/usr/bin/env bash

#cd /mnt/disks/curwsl_nfs/mike/outputs
#dirs=(/mnt/disks/curwsl_nfs/mike/outputs/*/)
today=$(date -u -d '+5 hour +30 min' '+%F')
dirs=$(find /mnt/disks/wrf_nfs/mike/outputs/ -name "$today"\*)
#echo $dirs
#for dir in "${dirs[@]}";
for dir in $dirs;
do
    echo "##########"
    echo "$dir"
    file_name="$dir/resmike11_WL.csv"
    echo $file_name
    FILE_MODIFIED_TIME=$(date -r ${file_name} +%s)
    CURRENT=$(date +%s)

    DIFF=$(((CURRENT-FILE_MODIFIED_TIME)/60))
    echo $DIFF

    if [ $DIFF -lt 60 ]
    then
      fgt=$(echo "$dir" | grep -oE "[^//]+$")
      echo $fgt

      IFS='_' read -r -a array <<< $fgt
      date=${array[0]}

      IFS='-' read -r -a array2 <<< ${array[1]}
      time="${array2[0]}:${array2[1]}:${array2[2]}"

      formatted_fgt="\"${date} ${time}\""

      echo $date
      echo $time
      echo $formatted_fgt

      echo $formatted_fgt
      /home/uwcc-admin/curw_mike_data_handler/outputs/extract_water_level.py -m 'mike11_2016' -t 'hourly_run' -f "${formatted_fgt}" -d "${dir}"
    fi
done
