#!/bin/bash

function toGetAllFilenames() { # output_filename ($1)
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

  head -n $(($start_index + $batch_size)) ../$file | tail -n $batch_size > batch.txt

  while read link_with_spaces
  do
    link=$(echo $link_with_spaces | tr -d ' ')
    wget $link
    echo $link >> ../$temporal_state_filename
    echo "----------" >> ../$temporal_state_filename
  done < batch.txt

  rm batch.txt

  cd ..
}

function toUploadToGoogleDrive() { # temporal_directory ($1), google_drive_dir ($2)
  temporal_directory=$1
  google_drive_dir=$2

  rclone copy $temporal_directory gdrive:$google_drive_dir
}

function toDeleteFolder() { # temporal_directory ($1)
  temporal_directory=$1

  rm -r $temporal_directory
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
for i in $(seq 1 $BATCH_SIZE $lines_count)
do
  echo [$i/$lines_count] Processing batch...
  toDownloadBatch filenames.txt $i $BATCH_SIZE tmp tmp_filenames.txt
  #toUploadToGoogleDrive tmp DatasetsProfiler/input/Edgar
  #toDeleteFolder tmp
  #toWriteProcessedLinks tmp_filenames.txt state.txt
  sleep 1
  echo Batch successfully processed
done