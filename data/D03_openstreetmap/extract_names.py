#!/usr/bin/python
# -*- coding: utf-8 -*-

from imposm.parser import OSMParser # https://imposm.org/docs/imposm.parser/latest/install.html
# apt-get install build-essential python-dev protobuf-compiler libprotobuf-dev
import sqlite3
import quadkey


import argparse
p = argparse.ArgumentParser()
p.add_argument('--output', type=str, required=True) 
p.add_argument('--input',  type=str, required=True) 
args = p.parse_args()

conn = sqlite3.connect(args.output)
cur = conn.cursor()

# TABLE作成
ways_ddl = """CREATE TABLE IF NOT EXISTS way_names(
    osmid INTEGER PRIMARY KEY,
    name_ja TEXT,
    refs TEXT
    )"""
cur.execute(ways_ddl)

# ways_dml
ways_dml = """INSERT OR IGNORE INTO way_names(
        'osmid',
        'name_ja',
        'refs')
        VALUES (?, ?, ?)"""

nodes_ddl = """CREATE TABLE IF NOT EXISTS node_names(
    osmid INTEGER PRIMARY KEY,
    name_ja TEXT,
    refs TEXT
    )"""
cur.execute(nodes_ddl)

# nodes_dml
nodes_dml = """INSERT OR IGNORE INTO node_names(
        'osmid',
        'name_ja',
        'refs')
        VALUES (?, ?, ?)"""

coords_ddl = """CREATE TABLE IF NOT EXISTS coord_names(
    osmid INTEGER PRIMARY KEY,
    qkey TEXT,
    longitude FLOAT,
    latitude FLOAT
    )"""
cur.execute(coords_ddl)
# INDEX
cur.execute("CREATE INDEX IF NOT EXISTS coord_names_qkey ON coord_names(qkey)")

# coords_dml
coords_dml = """INSERT OR IGNORE INTO coord_names(
        'osmid',
        'qkey',
        'longitude',
        'latitude')
        VALUES (?, ?, ?, ?)"""

class NameFetcher(object):
    def ways(self, ways):
        for osmid, tags, refs in ways:
            if 'name:ja' in tags and len(refs) > 0 :
                cur.execute(ways_dml, (osmid, tags['name:ja'], ",".join([str(x) for x in refs])))
    def nodes(self, nodes):
        for osmid, tags, refs in nodes:
            if 'name:ja' in tags > 0 :
                cur.execute(nodes_dml, (osmid, tags['name:ja'], ",".join([str(x) for x in refs])))
    def coords(self, coords):
        for osmid, lon, lat in coords:
            qkey =  quadkey.from_geo((lat,lon), 16).key
            cur.execute(coords_dml, (osmid, qkey, lon, lat))

# instantiate counter and parser and start parsing
name_fetcher = NameFetcher()
p = OSMParser(  concurrency=4,
                ways_callback=name_fetcher.ways,
                nodes_callback=name_fetcher.nodes,
                coords_callback=name_fetcher.coords )
p.parse(args.input)


# DB を確定
conn.commit()
conn.close()

