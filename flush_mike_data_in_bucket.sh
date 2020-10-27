files=$(gsutil ls gs://wrf_nfs/mike/inputs/*/*.txt)
files+=$(gsutil ls gs://wrf_nfs/mike/outputs/*/*.*)

NOW=$(date +%s)

for file in $files;
do
  LAST_MODIFIED_DATE=$(date -r $file +%s)
  DIFF=$(((NOW-LAST_MODIFIED_DATE)/60/60/24))
  echo $DIFF
  if [ $DIFF -gt 60 ]
  then
    echo $file
    rm -vr $file
  fi
done
