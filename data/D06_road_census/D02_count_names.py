#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3
import argparse

import csv

p = argparse.ArgumentParser()

p.add_argument('--roaddef', type=str, required=True) 
p.add_argument('--osmdb',  type=str, required=True) 
args = p.parse_args()

conn = sqlite3.connect(args.osmdb)
cur = conn.cursor()

query = "select count(*) from way_names where name = ?"

with open(args.roaddef, 'r') as f:
    reader = csv.reader(f)

    for road in reader:
        name = road[0].decode('utf-8')
        cur.execute(query, (name,))
        for row in cur:
            print ",".join(map(str, (name.encode('utf-8'),) + row))

conn.close()
