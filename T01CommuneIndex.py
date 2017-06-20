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

population = {
u"札幌市": 1936016,
u"旭川市": 347207,
u"函館市": 271479,
u"釧路市": 178394,
u"苫小牧市": 174064,
u"帯広市": 168753,
u"小樽市": 125028,
u"北見市": 122198,
u"江別市": 120225,
u"千歳市": 95532,
u"室蘭市": 89799,
u"岩見沢市": 86054,
u"恵庭市": 68956,
u"北広島市": 59629,
u"石狩市": 59362,
u"登別市": 50571,
u"北斗市": 47967,
u"音更町": 45391,
u"滝川市": 41924,
u"網走市": 37740,
u"稚内市": 36827,
u"伊達市": 35802,
u"名寄市": 29099,
u"七飯町": 28785,
u"根室市": 28050,
u"幕別町": 27660,
u"新ひだか町": 24295,
u"中標津町": 24205,
u"美唄市": 23984,
u"紋別市": 23644,
u"富良野市": 23324,
u"留萌市": 22957,
u"深川市": 22278,
u"遠軽町": 21432,
u"美幌町": 20851,
u"士別市": 20676,
u"釧路町": 20329,
u"余市町": 20152,
u"芽室町": 19218,
u"白老町": 18378,
u"砂川市": 18112,
u"八雲町": 17852,
u"当別町": 17251,
u"森町": 17004,
u"別海町": 15847,
u"倶知安町": 15825,
u"芦別市": 15404,
u"岩内町": 13770,
u"浦河町": 13289,
u"日高町": 12913,
u"栗山町": 12689,
u"斜里町": 12186,
u"長沼町": 11489,
u"赤平市": 11383,
u"上富良野町": 11263,
u"美瑛町": 10593,
u"東神楽町": 10237,
u"厚岸町": 10173,
u"清水町": 9896,
u"湧別町": 9535,
u"三笠市": 9519,
u"洞爺湖町": 9508,
u"夕張市": 9440,
u"むかわ町": 8997,
u"せたな町": 8845,
u"枝幸町": 8722,
u"白糠町": 8638,
u"安平町": 8555,
u"江差町": 8466,
u"松前町": 8251,
u"南幌町": 8155,
u"標茶町": 8007,
u"東川町": 7994,
u"弟子屈町": 7877,
u"本別町": 7733,
u"大空町": 7708,
u"羽幌町": 7552,
u"広尾町": 7468,
u"足寄町": 7376,
u"鷹栖町": 7264,
u"池田町": 7231,
u"新十津川町": 6929,
u"当麻町": 6834,
u"新得町": 6455,
u"士幌町": 6395,
u"共和町": 6352,
u"浜中町": 6282,
u"長万部町": 5972,
u"奈井江町": 5850,
u"大樹町": 5845,
u"新冠町": 5735,
u"今金町": 5730,
u"羅臼町": 5678,
u"佐呂間町": 5644,
u"由仁町": 5624,
u"鹿追町": 5603,
u"標津町": 5460,
u"上ノ国町": 5443,
u"平取町": 5416,
u"訓子府町": 5323,
u"中富良野町": 5272,
u"浦幌町": 5257,
u"津別町": 5231,
u"小清水町": 5227,
u"えりも町": 5153,
u"蘭越町": 5030,
u"ニセコ町": 4983,
u"上士幌町": 4924,
u"増毛町": 4893,
u"知内町": 4797,
u"美深町": 4727,
u"厚真町": 4711,
u"様似町": 4703,
u"雄武町": 4700,
u"木古内町": 4683,
u"福島町": 4669,
u"清里町": 4380,
u"豊浦町": 4306,
u"厚沢部町": 4258,
u"鹿部町": 4237,
u"豊富町": 4172,
u"興部町": 4106,
u"中札内村": 4080,
u"乙部町": 4059,
u"上川町": 4012,
u"浜頓別町": 3933,
u"比布町": 3924,
u"歌志内市": 3833,
u"和寒町": 3699,
u"月形町": 3577,
u"仁木町": 3518,
u"上砂川町": 3498,
u"下川町": 3494,
u"古平町": 3431,
u"小平町": 3394,
u"苫前町": 3390,
u"剣淵町": 3359,
u"豊頃町": 3359,
u"更別村": 3334,
u"沼田町": 3334,
u"新篠津村": 3333,
u"天塩町": 3324,
u"寿都町": 3258,
u"妹背牛町": 3241,
u"京極町": 3215,
u"置戸町": 3138,
u"愛別町": 3106,
u"黒松内町": 3097,
u"奥尻町": 2939,
u"遠別町": 2901,
u"滝上町": 2847,
u"猿払村": 2783,
u"利尻富士町": 2749,
u"礼文町": 2726,
u"壮瞥町": 2705,
u"雨竜町": 2682,
u"南富良野町": 2650,
u"秩父別町": 2614,
u"陸別町": 2596,
u"鶴居村": 2532,
u"幌延町": 2501,
u"喜茂別町": 2401,
u"積丹町": 2334,
u"利尻町": 2236,
u"真狩村": 2156,
u"浦臼町": 2078,
u"北竜町": 2041,
u"留寿都村": 1886,
u"中頓別町": 1863,
u"泊村": 1765,
u"中川町": 1708,
u"島牧村": 1631,
u"幌加内町": 1620,
u"初山別村": 1283,
u"占冠村": 1218,
u"西興部村": 1147,
u"赤井川村": 1139,
u"神恵内村": 946,
u"音威子府村": 800,
u"蘂取村": 0,
u"紗那村": 0,
u"留別村": 0,
u"色丹村": 0,
u"留夜別村": 0,
}

# =============

import luigi
from PIL import Image, ImageDraw, ImageFont

class mainTask(luigi.Task):
    mesh_data = luigi.Parameter(default="./data/D01_geomap/hokkaido.json")
    output_db = luigi.Parameter(default="./var/T01_commune_quadkey_map.db")
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

        # 人口ソートする
        hokkaido_geojson["features"].sort(key=lambda x : -population[get_commune_attributes(x)[1]])

        # 北海道を仮描画するキャンバスを用意する
        pixel_nw = deg_to_pixel_coordinates( *(EDGE_NW+(zoom,) ) )
        pixel_se = deg_to_pixel_coordinates( *(EDGE_SE+(zoom,) ) )
        img_size = (abs(pixel_nw[0] - pixel_se[0]) + 1, abs(pixel_nw[1] - pixel_se[1]) + 1) # w*h

        # キーマップ用画像作成
        img = Image.new('RGBA', img_size, (255, 255, 0, 255))
        draw = ImageDraw.Draw(img)
        # 太く塗る
        for feature in hokkaido_geojson["features"]:
            coordinates = [(x[1], x[0]) for x in feature["geometry"]["coordinates"][0]]
            projected_coordinates = [sub_offset(deg_to_pixel_coordinates(*(x+(zoom,))), pixel_nw) for x in coordinates]
            commune_id = int(feature["properties"]["N03_007"])
            r = int(commune_id/100)
            g = commune_id % 100
            draw.line(projected_coordinates, fill=(r,g,255,255), width=3)

        # 細く塗る
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
                tile_x = tile[0][0] * 256 + tile[1][0]
                tile_y = tile[0][1] * 256 + tile[1][1]
                qkey = quadkey.from_tile((tile_x, tile_y), 16).key
                cur.execute(dml_cq, (qkey, commune_info["commune_id"]))
                #print "{} {} {} {} {} {}".format(qkey, commune_info["commune"].encode('utf_8'), deg[0], deg[1],  tile_x, tile_y)

        # DB を確定
        conn.commit()
        conn.close()

if __name__ == "__main__":
    luigi.run(['mainTask', '--workers=1', '--local-scheduler'])
