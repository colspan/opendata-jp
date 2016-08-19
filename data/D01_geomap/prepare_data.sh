#!/bin/sh

# package for ubuntu
#sudo apt-get install gdal-bin

# 事前にzip配下にデータをダウンロードしておくこと
unzip -j zip/N03*GML.zip
ogr2ogr -f "GeoJSON" -lco COORDINATE_PRECISION=5 hokkaido.json N03*.shp

rm KS-META* N03*
