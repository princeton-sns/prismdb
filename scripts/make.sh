#!/bin/sh

# remove trailing slash in path
repo_path=${1%/}
optane_prism_path=${2%/}
qlc_prism_path=${3%/}

#echo "YOYO $repo_path"

cd $repo_path/build
#cmake --build . --target clean
cmake -DCMAKE_BUILD_TYPE=Release .. && cmake --build .

#echo $optane_prism_path
#echo $qlc_prism_path
sudo rm -rf $optane_prism_path/*
sudo rm -rf $qlc_prism_path/*

# sleep for 60 secs to allow files to be removed
sleep 60
