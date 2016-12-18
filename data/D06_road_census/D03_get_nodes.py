#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import argparse

import csv
from collections import OrderedDict

import json

p = argparse.ArgumentParser()

p.add_argument('--roadnamedef', type=str, required=True) 
p.add_argument('--osmdb',  type=str, required=True) 
args = p.parse_args()

conn = sqlite3.connect(args.osmdb)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

cur2 = conn.cursor()

query_way_name_to_way_id = "select wname.id as way_id, wname.name as name from way_names as wname where wname.name = ?"

query_way_id_to_nodes = "select nn.id,nn.latitude,nn.longitude from node_names as nn inner join (select wn_j.node_id as node_id, wname.id as way_id, wname.name as name from way_node as wn_j inner join way_names as wname on wn_j.way_id = wname.id where way_id = ?) as j on j.node_id = nn.id"

names = OrderedDict()

with open(args.roaddef, 'r') as f:
    reader = csv.reader(f)

    # 道路名
    for road in reader:
        ways = OrderedDict()
        name = road[0].decode('utf-8')
        cur.execute(query_way_name_to_way_id, (name,))
        for way in cur:
            cur2.execute(query_way_id_to_nodes, (way['way_id'],))
            nodes = OrderedDict()
            for node in cur2:
                nodes[node['nn.id']] = {"lat":node['nn.latitude'], "lon":node['nn.longitude']}
            ways[way['way_id']] = nodes
        names[name] = ways

conn.close()

features = []
for name, ways in names.items():
    # 各路線ごとに
    joined_pairs = {}
    print name.encode('utf-8')
    for way_id, nodes in ways.items():
        # 連結するwayを探す
        ## 始点
        start_node = nodes.items()[0]
        pair_way_id_s = None
        for wid, nds in ways.items():
            if wid == way_id:
                continue
            if nds.items()[0][0] == start_node[0]:
                pair_way_id_s = (wid, 'start') 
            if nds.items()[-1][0] == start_node[0]:
                pair_way_id_s = (wid, 'end') 
        ## 終点
        end_node = nodes.items()[-1]
        pair_way_id_e = None
        for wid, nds in ways.items():
            if wid == way_id:
                continue
            if nds.items()[0][0] == end_node[0]:
                pair_way_id_e = (wid, 'start') 
            if nds.items()[-1][0] == end_node[0]:
                pair_way_id_e = (wid, 'end') 
        joined_pairs[way_id] = (pair_way_id_s, pair_way_id_e)
    # Noneが片方だけのペアを探す
    pairs_includes_none = []
    for wid, jp in joined_pairs.items():
        if jp[0] == None and jp[1] != None or jp[0] != None and jp[1] == None:
            pairs_includes_none.append((wid,)+jp)
    #print pairs_includes_none
    # Noneが片方だけのペアが３個以上あったら諦める
    if len(pairs_includes_none) != 2:
        print 'skipped {}'.format(name.encode('utf-8'))
        continue
    # 順に舐めてnodeを拾い集める
    nodes = []
    processed_ids = []
    last_way = None
    current_way = pairs_includes_none[0][0]
    while True:
        if current_way in processed_ids:
            # 無限ループを回避
            break
        # nodeを積む（last_wayの方から順に）
        # 結合先はlast_wayでないほうのway
        if last_way == joined_pairs[current_way][0]:
            # start
            nodes += ways[current_way].values()
            try:
                next_way = joined_pairs[current_way][1][0]
            except TypeError:
                next_way = None
        else:
            # end
            nodes += reversed(ways[current_way].values())
            try:
                next_way = joined_pairs[current_way][0][0]
            except TypeError:
                next_way = None
        processed_ids.append(current_way)
        last_way = current_way
        current_way = next_way
        if current_way == None:
            break
    print nodes
    feature = {
        "type":"Feature",
        "geometry":{
            "type":"MultiPoint",
            "coordinates": [[x['lon'], x['lat']] for x in nodes ]
        },
        "properties":{"name":name}
    }
    features.append(feature)

geojson_obj = {
    "type":"FeatureCollection",
    "features":features
}


with open("output.json", "w") as f:
    json.dump(geojson_obj, f)
