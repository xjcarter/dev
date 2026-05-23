#!/bin/bash
 
# Set the base directories
trading_dir="/trading"
portfolio_dir="/portfolio"
archives_dir="/archives"

# Set the current date in 'YYYYMMDD' format
current_date=$(date +'%Y%m%d')

# Map out the current file tree 
cd /
echo "" > "$archives_dir/prod_file_tree.txt"
echo "production tree map -> starting from /" >> "$archives_dir/prod_file_tree.txt"
echo "" >> "$archives_dir/prod_file_tree.txt"
tree -d root >> "$archives_dir/prod_file_tree.txt"
tree -d trading >> "$archives_dir/prod_file_tree.txt"
tree -d portfolio >> "$archives_dir/prod_file_tree.txt"
tree -d archives >> "$archives_dir/prod_file_tree.txt"

# Copy over current crontab
cp /trading/misc/crontabs/crontab.CURRENT $archives_dir/

# Copy over listing of current docker images
docker images > "$archives_dir/docker_images.txt"

# Create gzip tarball for /trading
trading_tarball="$archives_dir/trading/trading.$current_date.gz"
tar -czvf "$trading_tarball" -C "$trading_dir" .
# Create symlink
rm "$archives_dir/trading/current"
ln -s $trading_tarball "$archives_dir/trading/current"

echo "Created /trading tarball: $trading_tarball"

# Create gzip tarball for /portfolio
portfolio_tarball="$archives_dir/portfolio/portfolio.$current_date.gz"
tar -czvf "$portfolio_tarball" -C "$portfolio_dir" .
# Create symlink
rm "$archives_dir/portfolio/current"
ln -s $portfolio_tarball "$archives_dir/portfolio/current"

echo "Created /portfolio tarball: $portfolio_tarball"

