#!/bin/bash

echo
if [ $# -lt 1 ]
then
    echo "Uaage: fetch_data.sh <fetch_list_file>"
else
    #fetch_file="${DATA_DIR}/${1}"
    fetch_file=$1
    if [ ! -f ${fetch_file} ]
    then
        echo "fetch_list_file: ${fetch_file} not found."
        exit 1
    else
        echo "Using fetch_file ${fetch_file} ..."
        python3 /trading/utils/yahoo_data.py --file="${fetch_file}" 
        python3 /trading/utils/data_catalog.py --file="${fetch_file}" 
    fi
fi



