#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import argparse

import csv
from collections import OrderedDict

p = argparse.ArgumentParser()

p.add_argument('--roaddef', type=str, required=True) 
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

#for name, ways in names.items():
#    print name
