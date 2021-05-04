#!/bin/bash
# Execute from Dataset Profiler root directory

export SCRIPTS_PATH=scripts

export CORRECT=output/samples_correct
export TEST=output/samples_test

export RED='\033[0;31m'
export GREEN='\033[92m'
export END='\033[0m'

rm -r output/samples_test/*

$SCRIPTS_PATH/run-local.sh parameters/parameters_integration_test

if [ "$(diff -bur $CORRECT $TEST | grep -v "Only in" -c)" -eq 0 ];
then
  printf "%sAll tests passed successfully! %s\n" "${GREEN}" "${END}"
else
  diff -bur $CORRECT $TEST | grep -v "Only in" | less
  printf "%sTests did not pass %s\n" "${RED}" "${END}"
fi;
