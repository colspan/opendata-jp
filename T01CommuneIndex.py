#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import json
import sqlite3
import quadkey

from sympy.geometry import Point, Polygon

import numpy as np

# 参考文献
# http://qiita.com/s-wakaba/items/f414bed3736dc5b368c8
from math import sin, cos, tan, acos, asin, atan2, radians, degrees, pi, log, atan, sinh

def latlng_to_xyz(lat, lng):
    rlat, rlng = radians(lat), radians(lng)
    coslat = cos(rlat)
    return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)

def xyz_to_latlng(x, y, z):
    rlat = asin(z)
    coslat = cos(rlat)
    return degrees(rlat), degrees(atan2(y/coslat, x/coslat))

def halfway_on_sphere(pos0, pos1, z=0.5):
    xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)

    theta = acos(sum(x * y for x, y in zip(xyz0, xyz1)))
    sin_th = sin(theta)

    v0 = sin(theta * (1-z)) / sin_th
    v1 = sin(theta * z) / sin_th

    return xyz_to_latlng(*(x * v0 + y * v1 for x, y in zip(xyz0, xyz1)))

def num_to_deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = atan(sinh(pi * (1 - 2 * ytile / n)))
    lat_deg = degrees(lat_rad)
    return (lat_deg, lon_deg)

def deg_to_num(lat_deg, lon_deg, zoom):
    lat_rad = radians(lat_deg)
    n = 2.0 ** zoom
    xtile_f = (lon_deg + 180.0) / 360.0 * n
    ytile_f = (1.0 - log(tan(lat_rad) + (1 / cos(lat_rad))) / pi) / 2.0 * n
    xtile = int(xtile_f)
    ytile = int(ytile_f)
    pos_x = int((xtile_f - xtile)*256)
    pos_y = int((ytile_f - ytile)*256)
    return (xtile, ytile, pos_x, pos_y)

def deg_to_pixel_coordinates(lat_deg, lon_deg, zoom):
    sin_lat = sin(lat_deg * pi / 180.0)
    n = 2.0 ** zoom
    pixel_x = int(((lon_deg + 180.0) / 360.0) * 256 * n)
    pixel_y = int((0.5-log((1.0+sin_lat)/(1.0-sin_lat))/(4.0*pi))*256 * n)
    return (pixel_x, pixel_y)

def pixel_to_tile(pixelX, pixelY):
    return (( int(pixelX / 256), int(pixelY / 256)), ( pixelX % 256, pixelY % 256))

# ==============
# 北海道全域
EDGE_NW = (45.80000, 139.05524)
EDGE_SE = (41.23064, 150.20000)
zoom = 8

# ==============

def get_commune_attributes(feature):
    if feature["properties"]["N03_003"] == u"札幌市":
        county = feature["properties"]["N03_003"]
        commune = county
        ward = feature["properties"]["N03_004"]
    elif feature["properties"]["N03_003"] == None: # 市
        county = feature["properties"]["N03_004"]
        commune = feature["properties"]["N03_004"]
        ward = None
    else:
        county = feature["properties"]["N03_003"]
        commune = feature["properties"]["N03_004"]
        ward = None
    return (county, commune, ward)

def sub_offset(target, offset):
    return ((target[0]-offset[0], target[1]-offset[1]))

# =============

import luigi
from PIL import Image, ImageDraw, ImageFont

class mainTask(luigi.Task):
    mesh_data = luigi.Parameter(default="./data/D01_geomap/hokkaido.json")
    output_db = luigi.Parameter(default="./var/N01_commune_quadkey_map.db")
    def output(self):
        return luigi.LocalTarget(self.output_db)
    def run(self):
        # GeoJSON読み込み
        with open(self.mesh_data, "r") as f:
            hokkaido_geojson = json.load(f)

        # 市町村名にIDを付与
        communes = {}
        for feature in hokkaido_geojson["features"]:
            county, commune, ward = get_commune_attributes(feature)
            commune_id = int(feature["properties"]["N03_007"])
            communes[commune_id] = {
                "county"  :county,
                "commune" :commune,
                "ward"    :ward,
                "commune_id": commune_id,
                #"original":feature
            }

        # 北海道を仮描画するキャンバスを用意する
        pixel_nw = deg_to_pixel_coordinates( *(EDGE_NW+(zoom,) ) )
        pixel_se = deg_to_pixel_coordinates( *(EDGE_SE+(zoom,) ) )
        img_size = (abs(pixel_nw[0] - pixel_se[0]) + 1, abs(pixel_nw[1] - pixel_se[1]) + 1) # w*h

        # キーマップ用画像作成
        img = Image.new('RGBA', img_size, (255, 255, 0, 255))
        draw = ImageDraw.Draw(img)
        for feature in hokkaido_geojson["features"]:
            coordinates = [(x[1], x[0]) for x in feature["geometry"]["coordinates"][0]]
            projected_coordinates = [sub_offset(deg_to_pixel_coordinates(*(x+(zoom,))), pixel_nw) for x in coordinates]
            commune_id = int(feature["properties"]["N03_007"])
            r = int(commune_id/100)
            g = commune_id % 100
            draw.polygon(projected_coordinates, fill=(r,g,255,255))

        img.save( self.output().fn + '.png', 'PNG')

        # DB準備
        conn = sqlite3.connect(self.output().fn)
        cur = conn.cursor()

        # TABLE作成
        ddl_c = """CREATE TABLE IF NOT EXISTS communes(
            commune_id INTEGER PRIMARY KEY,
            county TEXT,
            commune TEXT,
            ward TEXT
            )"""
        cur.execute(ddl_c)

        dml_c = """INSERT INTO communes(
                commune_id,
                county,
                commune,
                ward)
                VALUES (?, ?, ?, ?)"""

        # 自治体マスタテーブルデータ登録
        for commune_info in communes.values():
            cur.execute(dml_c, (commune_info["commune_id"], commune_info["county"], commune_info["commune"], commune_info["ward"]))

        # 画像から qkey と自治体IDのひも付けDB作成
        ddl_cq = """CREATE TABLE IF NOT EXISTS commune_qkey(
            qkey TEXT PRIMARY KEY,
            commune_id INTEGER
            )"""
        cur.execute(ddl_cq)

        dml_cq = """INSERT INTO commune_qkey(
                qkey,
                commune_id)
                VALUES (?, ?)"""

        for y in range(img_size[1]):
            for x in range(img_size[0]):
                pixel_info = img.getpixel((x, y))
                if pixel_info[2] == 0:
                    # データがなければ次へ
                    continue
                commune_id = pixel_info[0] * 100 + pixel_info[1]
                commune_info = communes[commune_id]
                tile = pixel_to_tile(x+pixel_nw[0], y+pixel_nw[1])
                tile_x = tile[0][0] + float(tile[1][0])/256.0
                tile_y = tile[0][1] + float(tile[1][1])/256.0
                deg = num_to_deg(tile_x, tile_y, zoom)
                qkey = quadkey.from_geo(deg, 16).key
                cur.execute(dml_cq, (qkey, commune_info["commune_id"]))
                #print "{} {} {} {} {} {}".format(qkey, commune_info["commune"].encode('utf_8'), deg[0], deg[1],  tile_x, tile_y)

        # DB を確定
        conn.commit()
        conn.close()

if __name__ == "__main__":
    luigi.run(['mainTask', '--workers=1', '--local-scheduler'])
