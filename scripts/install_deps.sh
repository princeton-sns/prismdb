#!/bin/sh

sudo apt-get update -y
sudo apt install -y google-perftools libgoogle-perftools-dev
git pull --recurse-submodules
sudo apt install matplotlib
sudo apt install python3-pip
sudo apt-get install python3-matplotlib
sudo apt  install cmake
sudo apt install cgroup-tools
sudo apt-get install libsnappy-dev
sudo apt install libtbb-dev
sudo apt install sysstat

sudo mv /bin/sh /bin/sh.orig
sudo ln -s /bin/bash /bin/sh

# for rocksdb
sudo apt-get install libgflags-dev
