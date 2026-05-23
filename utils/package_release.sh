#!/bin/bash

# this script tarballs all release code and sends it over to the receiving cloudbox
# once on the recieving box- use unpack_release.sh
#

usage() 
{
    echo "Usage: $0 <destination box>"
    echo "  indicate destination box as required parameter"
    exit 1
}

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
	usage
fi

hostnames="topsy surus"
password='Eleph@ntTusk123$'

# Get the destination hostname
dest=$1

valid_box=false
# Check if the current hostname is in the array
for name in ${hostnames}; do
    if [ "$dest" == "$name" ]; then
        valid_box=true 
    fi
done

if [ "$valid_box" = false ]; then
    echo "invalid destination: ${dest}"
    exit 1
fi

current_box=$(hostname)
current_date=$(date +"%Y%m%d")

#for cloud boxes
base_dir="/"
#for local pc
if [ "$current_box" == "MacBook-Pro" ]; then
    base_dir="/Users/jcarter/hannibal/"
fi

# package code directories to temp directory
temp_dir=$(mktemp -d)
release_package="${temp_dir}/release.${current_date}.gz"
tar -czvf "${release_package}" -C ${base_dir} trading/lib/ trading/utils/ trading/misc/ trading/strats/
echo "writing archive ${release_package}"

# send release package to the destination cloud box
# package sent to /archives/releases
sshpass -p ${password} scp ${release_package} root@${dest}:/archives/releases/
echo "release: ${release_package} sent to: ${dest}:/archives/releases"

# remove temp dir and tarball
rm -fr ${temp_dir}
