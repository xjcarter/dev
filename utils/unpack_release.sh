#!/bin/bash

usage() 
{
    echo "Usage: $0 <release_tarball>"
    echo "  indicate desired release tarball inside /archive/releases/"
    ls -ltrh /archives/releases/
    exit 1
}

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
	usage
fi

release_tarball=$1

archive_repo="/archives/releases"
new_release=${archive_repo}/${release_tarball}
tar xvf ${new_release} -C ${archive_repo}
echo "Unpacked ${new_release}..."

echo "Backing up current /trading repo to /archives/rollback..."
rm -fr /archives/rollback/trading
cp -pr /trading /archives/rollback/

echo "updating and intergrating new release into production:"
echo "Targetted modifications:"
/trading/utils/clean_copy.sh -t /archives/releases/trading /trading

# prompt the user
read -p "Do you want to proceed with the release? (Y/N): " user_response

# check the user's response
if [ "$user_response" == "Y" ]; then
    echo "Executing release..."
    /trading/utils/clean_copy.sh /archives/releases/trading /trading
else
    echo "Release aborted."
fi

# cleaning up expanded archive files
rm -fr /archives/releases/trading
