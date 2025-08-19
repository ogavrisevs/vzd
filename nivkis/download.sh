#!/bin/bash

echo "Downloading datasets"

DATA="https://data.gov.lv/dati"
DATASET="be841486-4af9-4d38-aa14-6502a2ddb517"

declare -a resources=("2aeea249-6948-4713-92c2-e01543ea0f33") 

resource_length=${#resources[@]}

for (( i=0; i<${resource_length}; i++ ));
do
  echo "Downloading ${years[$i]}"
  wget $DATA"/dataset/"$DATASET"/resource/${resources[$i]}/download/address.zip"
done

EXTRACT_DIR="all"

echo "Extracting zip "
for file in address.zip
do
    unzip $file -d $EXTRACT_DIR
done
