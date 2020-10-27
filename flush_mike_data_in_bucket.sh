NOW=$(date +%s)

cd /mnt/disks/wrf_nfs/mike/inputs

for dir in $(ls -d */)
do
  LAST_MODIFIED_DATE=$(date -r $dir +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    echo $dir
    rm -vr $dir
  fi
done

cd /mnt/disks/wrf_nfs/mike/outputs

for dir in $(ls -d */)
do
  LAST_MODIFIED_DATE=$(date -r $dir +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    echo $dir
    rm -vr $dir
  fi
done