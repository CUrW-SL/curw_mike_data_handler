files=$(gsutil ls gs://wrf_nfs/mike/inputs/*/*.txt)
files+=$(gsutil ls gs://wrf_nfs/mike/outputs/*/*.*)

NOW=$(date +%s)

for file in $files;
do
  echo $file
  # split string by delimiter '://'
  IFS='://' read -ra LIST <<< "$file"
  echo $LIST

  # first element in array
  filename="/mnt/disks/${LIST[1]}"
  echo $filename

  LAST_MODIFIED_DATE=$(date -r $filename +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    rm -v $filename
  fi
done
