#!/bin/sh

# 事前にzip配下にデータをダウンロードしておくこと

for i in $(ls zip); do
  unzip zip/$i
done

cat tblT*.txt | sort | grep -v KEY_CODE | grep -v ^, > population_mesh_third_half.csv
rm tblT*.txt
