#!/bin/bash
# Execute from Dataset Profiler root directory

export SCRIPTS_PATH=scripts

export CORRECT=output/samples_correct
export TEST=output/samples_test

rm -r output/samples_test/*

$SCRIPTS_PATH/run-local.sh parameters/parameters_integration_test.json

if [ "$(diff -bur $CORRECT $TEST | grep -v "Only in" -c)" -eq 0 ];
then
  echo "All tests passed successfully!"
else
  diff -bur $CORRECT $TEST | grep -v "Only in" | less
  echo "Tests did not pass"
fi;
