sample_sz=$1
if [ $# -lt 2 ]
then
    sample_sz=50
fi
python beta_map.py SPY --file=sp500_list.txt --sample_size=${sample_sz} --corr_above=0.80 --beta_above=1.0
