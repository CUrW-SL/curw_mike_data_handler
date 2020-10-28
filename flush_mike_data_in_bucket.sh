files=$(gsutil ls gs://wrf_nfs/mike/inputs/*/*.txt)

NOW=$(date +%s)

for file in $files;
do
  echo $file

  IFS='/' read -ra LIST <<< "$file"

  filename="/mnt/disks"
  for i in "${LIST[@]:2}"; do
    filename="$filename/$i"
  done
  echo $filename

  LAST_MODIFIED_DATE=$(date -r $filename +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    rm -vr $filename
  fi
done

files2=$(gsutil ls gs://wrf_nfs/mike/outputs/*/*.*)

NOW=$(date +%s)

for file in $files2;
do
  echo $file

  IFS='/' read -ra LIST <<< "$file"

  filename="/mnt/disks"
  for i in "${LIST[@]:2}"; do
    filename="$filename/$i"
  done
  echo $filename

  LAST_MODIFIED_DATE=$(date -r $filename +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    rm -vr $filename
  fi
done
