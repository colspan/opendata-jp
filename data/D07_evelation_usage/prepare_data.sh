#!/bin/sh

# package for ubuntu
#sudo apt-get install gdal-bin

# 事前にzip配下にデータをダウンロードしておくこと
for i in zip/G04*.zip; do unzip -j $i; done;
for i in G04*.shp; do ogr2ogr -f "CSV" ${i%*.*}.csv $i; done;
cat G04*.csv | nkf | grep -v unknown > g04_hokkaido.csv

for i in zip/L03*.zip; do unzip -j $i; done;
chmod 644 *L03*
for i in L03*.shp; do ogr2ogr -f "CSV" ${i%*.*}.csv $i; done;
cat L03*.csv | nkf > l03_hokkaido.csv

rm -f KS-META* G04* L03*
