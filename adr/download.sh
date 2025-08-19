#!/bin/bash
set -x 

echo "Downloading current datasets"

DATA="https://data.gov.lv/dati"
DATASET="0c5e1a3b-0097-45a9-afa9-7f7262f3f623"
RESOURCE="1d3cbdf2-ee7d-4743-90c7-97d38824d0bf" 

echo "Downloading "
wget $DATA"/dataset/"$DATASET"/resource/"$RESOURCE"/download/aw_csv.zip"


EXTRACT_DIR="all/cur"
if [ ! -d "$EXTRACT_DIR" ]; then
  mkdir -p "$EXTRACT_DIR"
fi

echo "Extracting zip "
for file in aw_csv.zip
do
    unzip $file -d $EXTRACT_DIR
done


echo "Downloading historic datasets"

EXTRACT_DIR="all/his"
if [ ! -d "$EXTRACT_DIR" ]; then
  mkdir -p "$EXTRACT_DIR"
fi
DATA="https://data.gov.lv/dati"
DATASET="6b06a7e8-dedf-4705-a47b-2a7c51177473"

declare -a resources=("e7f17c92-fad4-4153-bef5-670a321c4ec1" "87e2c4e5-13d9-4142-9052-8a6e9f094479" "c5c3d570-1596-49f2-a486-53439b449641" "5950bf88-4441-470f-9e13-efcbd79bc1f0" "c8f34472-8ca4-40d5-9c84-05b24dc19afe" "a7461a4e-4407-4506-9333-a50c4f51b328" "d07443d7-15a8-4db6-9e53-7a68eec3c0dd" "26e63e84-c04d-40b5-9c37-0ca9d08789ad" "2dbe69b1-6b14-4f35-98b8-2a64119af163")

declare -a csvs=("aw_rajons.csv" "aw_pilseta_his.csv" "aw_novads_his.csv" "aw_pagasts_his.csv" "aw_ciems_his.csv" "aw_iela_his.csv" "aw_eka_his.csv" "aw_dziv_his.csv", "aw_doc_vieta.csv")
resource_length=${#resources[@]}

for (( i=0; i<${resource_length}; i++ ));
do
  echo "Downloading ${csvs[$i]}"
  wget $DATA"/dataset/"$DATASET"/resource/${resources[$i]}/download/${csvs[$i]}" -O "./${EXTRACT_DIR}/${csvs[$i]}"
done

