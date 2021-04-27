#!/bin/bash

function toGetAllFilenames() {
  output_filename=$1

  curl https://www.sec.gov/files/EDGAR_LogFileData_thru_Jun2017.html | grep "www" > $output_filename
}

function toDownloadBatch() { # file ($1), file index ($2), batch size ($3), output_directory ($4), temporal_state_filename ($5)
  file=$1
  start_index=$2
  batch_size=$3
  output_directory=$4
  temporal_state_filename=$5

  mkdir -p $output_directory
  cd $output_directory

  filenames=$(head -n $(($start_index + $batch_size)) ../$file | tail -n $batch_size)
  readarray -t filenames_array < <(echo $filenames)
  for link in "${filenames_array[@]}"
  do
    wget $link
    echo $link >> ../$temporal_state_filename
  done

  cd ..
}

function toUploadToGoogleDrive() {
  rclone copy tmp/* gdrive:DatasetsProfiler/input/Edgar
}

function toDeleteFolder() {
    rm tmp/*
}

function toWriteProcessedLinks() { # temporal_state_filename ($1), state_filename ($2)
    temporal_state_filename=$1
    state_filename=$2

    cat $temporal_state_filename >> $state_filename
    rm $temporal_state_filename
}

#toGetAllFilenames filenames.txt

export BATCH_SIZE=10

lines_count=$(cat filenames.txt | wc -l)
for i in $(echo {1..$lines_count..$BATCH_SIZE})
do
  toDownloadBatch filenames.txt $i $BATCH_SIZE tmp tmp_filenames.txt
  toUploadToGoogleDrive
  toDeleteFolder
  toWriteProcessedLinks tmp_filenames.txt state.txt
  wait 1
done