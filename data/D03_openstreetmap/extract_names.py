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

# ways
ways_ddl = """CREATE TABLE IF NOT EXISTS way_names(
    id INTEGER PRIMARY KEY,
    name TEXT
    )"""
cur.execute(ways_ddl)
ways_dml = """INSERT OR IGNORE INTO way_names(
    'id',
    'name')
    VALUES (?, ?)"""
cur.execute("CREATE INDEX IF NOT EXISTS way_names_name ON way_names(name)")


# way_node
way_node_ddl = """CREATE TABLE IF NOT EXISTS way_node(
    way_id INTEGER,
    node_id INTEGER
    )"""
cur.execute(way_node_ddl)
way_node_dml = """INSERT OR IGNORE INTO way_node(
    'way_id',
    'node_id')
    VALUES (?, ?)"""
cur.execute("CREATE INDEX IF NOT EXISTS way_node_way_id ON way_node(way_id)")
cur.execute("CREATE INDEX IF NOT EXISTS way_node_node_id ON way_node(node_id)")

# nodes
nodes_ddl = """CREATE TABLE IF NOT EXISTS node_names(
    id INTEGER PRIMARY KEY,
    name TEXT,
    qkey TEXT,
    longitude FLOAT,
    latitude FLOAT
    )"""
cur.execute(nodes_ddl)
nodes_dml = """INSERT OR IGNORE INTO node_names(
    'id',
    'name',
    'qkey',
    'longitude',
    'latitude'
    )
    VALUES (?, ?, ?, ?, ?)"""
# INDEX
cur.execute("CREATE INDEX IF NOT EXISTS node_names_qkey ON node_names(qkey)")
cur.execute("CREATE INDEX IF NOT EXISTS node_names_name ON node_names(name)")


## relations
#relations_ddl = """CREATE TABLE IF NOT EXISTS relations(
#    relation_id INTEGER,
#    type TEXT,
#    name TEXT,
#
#    )"""
#cur.execute(way_node_ddl)
#way_node_dml = """INSERT OR IGNORE INTO way_node(
#    'way_id',
#    'node_id')
#    VALUES (?, ?)"""


class NameFetcher(object):
    def ways(self, ways):
        for osmid, tags, refs in ways:
            if 'name' in tags:
                name = tags['name']
            else:
                name = None
            if 'name:ja' in tags:
                name = tags['name:ja']
            if len(refs) > 0 :
                cur.execute(ways_dml, (osmid, name))
                for ref in refs:
                    cur.execute(way_node_dml, (osmid, ref))
                    
    def nodes(self, nodes):
        for osmid, tags, coord in nodes:
            if 'name' in tags:
                name = tags['name']
            else:
                name = None
            if 'name:ja' in tags:
                name = tags['name:ja']
            lon, lat = coord
            qkey =  quadkey.from_geo((lat,lon), 16).key
            cur.execute(nodes_dml, (osmid, name, qkey, lon, lat))
    def coords(self, coords):
        for osmid, lon, lat in coords:
            qkey =  quadkey.from_geo((lat,lon), 16).key
            cur.execute(nodes_dml, (osmid, None, qkey, lon, lat))

# instantiate counter and parser and start parsing
name_fetcher = NameFetcher()
p1 = OSMParser( concurrency=4,
                nodes_callback=name_fetcher.nodes )
p1.parse(args.input)

p2 = OSMParser( concurrency=4,
                coords_callback=name_fetcher.coords )
p2.parse(args.input)

p3 = OSMParser( concurrency=4,
                ways_callback=name_fetcher.ways )
p3.parse(args.input)


# DB を確定
conn.commit()
conn.close()

