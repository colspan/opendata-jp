#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import argparse

import csv
from collections import OrderedDict

import json

p = argparse.ArgumentParser()

p.add_argument('--pathdef', type=str, required=True) 
p.add_argument('--roaddef',  type=str, required=True) 
args = p.parse_args()

names = OrderedDict()

with open(args.pathdef, "r") as f:
    geojson_obj = json.load(f)

"""
geojson_obj = {
    "type":"FeatureCollection",
    "features":features
}
"""

roads = {}

with open(args.roaddef, 'r') as f:
    reader = csv.reader(f)
    header = next(reader)
    # 道路名
    for r in reader:
        name = r[4].decode('utf-8')
        if not name in roads.keys():
            roads[name] = []
        #print r[0]
        roads[name].append({"id":int(r[0]),"distance":float(r[5]),"address":r[6]})

#print roads

def get_osm_road_path(name):
    for feature in geojson_obj["features"]:
        if feature["properties"]["name"] == name:
            return feature["geometry"]["coordinates"]
    return None

# roadsでループ
for name, points in roads.items():
    print points

    # geojsonの対応データを探す
    osm_path = get_osm_road_path(name)
    if osm_path == None:
        continue
    print osm_path
# ざっくり方角を求める
# geojsonから得られた緯度軽度列の端から距離を計算する
# roadsの端から距離を計算する
# 距離の乖離を計算する
# 近い点の緯度経度を算出する
# roadsに緯度経度の代表点を書き戻す

