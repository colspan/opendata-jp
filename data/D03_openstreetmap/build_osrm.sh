#!/bin/sh

sudo apt-get install -y cmake
sudo apt-get install -y libboost-all-dev libtbb-dev liblua5.2-dev libluabind-dev libstxxl-dev libxml2 libxml2-dev libosmpbf-dev libbz2-dev libprotobuf-dev
wget https://github.com/Project-OSRM/osrm-backend/archive/v5.1.0.zip
unzip v5.1.0.zip
cd osrm-backend-5.1.0
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
#sudo cmake --build . --target install

