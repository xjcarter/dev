#!/bin/bash

python yahoo_data.py --file="${DATA_DIR}/fetch_list.txt" 
python data_catalog.py --file="${DATA_DIR}/fetch_list.txt" 


