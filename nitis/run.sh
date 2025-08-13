#!/bin/bash

echo "Downloading datasets"

DATA="https://data.gov.lv/dati"
DATASET="f8a8a929-28d5-4f4f-85e9-062168cb4aba"

declare -a resources=("4fe41171-3313-4db3-bb2b-dea1f7809a21" "ac8958ca-f958-4830-9c23-9e753a49150c" "1d8cfa37-fc6d-4c27-9981-162190ca4556" "30863034-4272-4a38-99a2-8331757280d9" "a2ea453a-4f44-4d60-9739-8b050fdea77b" "56a67c0d-427c-49ff-814a-c90d557f40e5" "48fd44d1-d772-4311-ae03-fb380c25c9f5" "427b3f2f-1d27-4db6-b9b7-4bf7bc037cce" "42907737-86b7-475b-a498-e53946fa7341" "c89801cb-4acc-4c25-8874-6fb76ae7866c" "60b3e00a-514a-45a3-a95d-733a7501e33a" "4faa7768-6242-4db7-ae72-67a3ca85e99b" "0ef6aa48-9fa0-44a0-845a-96912851f2d7" 
"e06518db-9719-4a5e-b946-6796574c32db") 

declare -a years=("nitis_csv_2025.zip" "nitis_csv_2024.zip" "nitis_csv_2023.zip" "nitis_csv_2022.zip" "nitis_csv_2021.zip" "nitis_csv_2020.zip" "nitis_csv_2019.zip" "nitis_csv_2018.zip" "nitis_csv_2017.zip" "nitis_csv_2016.zip" "nitis_csv_2015.zip" "nitis_csv_2014.zip" "nitis_csv_2013.zip" "nitis_csv_2012.zip")
resource_length=${#resources[@]}

for (( i=0; i<${resource_length}; i++ ));
do
  echo "Downloading ${years[$i]}"
  wget $DATA"/dataset/"$DATASET"/resource/${resources[$i]}/download/${years[$i]}"
done

EXTRACT_DIR="all"
DB_NAME="vzd.db"

echo "Extracting zip "
for file in nitis_csv_*.zip
do
    unzip $file -d $EXTRACT_DIR
done

echo "Create db"
sqlite3 $DB_NAME < create-tables.sql

echo "Loading zv csv to sqlite3"
for file in ./$EXTRACT_DIR/ZV_CSV_*.csv
do
    echo "Loading $file"
    sqlite3 $DB_NAME ".separator ';'" ".import --skip 1 $file zv"
done

echo "Loading zvb csv to sqlite3"
for file in ./$EXTRACT_DIR/ZVB_CSV_*.csv
do
    echo "Loading $file"
    sqlite3 $DB_NAME ".separator ';'" ".import --skip 1 $file zvb"
done

echo "Loading tg csv to sqlite3"
for file in ./$EXTRACT_DIR/TG_CSV_*.csv
do
    echo "Loading $file"
    sqlite3 $DB_NAME ".separator ';'" ".import --skip 1 $file tg"
done

echo "Cleaning up"
for file in ./$EXTRACT_DIR/*.csv
do
    echo "Removing file $file"
    rm $file
done
for file in ./nitis_csv_*.zip
do
    echo "Removing file $file"
    rm $file
done
rmdir $EXTRACT_DIR

echo "Finito !"