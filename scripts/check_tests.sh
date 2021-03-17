#!/bin/bash

export CORRECT=../output/samples_correct
export TEST=../output/samples_test

export RED='\033[0;31m'
export GREEN='\033[92m'
export END='\033[0m'

if [ "$(diff -bur $CORRECT $TEST | grep -v "Only in" -c)" -eq 0 ];
then
  printf "${GREEN}All tests passed successfully! ${END}\n"
else
  printf "${RED}Error passing the tests ${END}\n"
fi;