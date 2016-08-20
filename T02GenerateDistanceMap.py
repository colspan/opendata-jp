#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import copy
import luigi
import pickle
import quadkey
import pandas
import sqlite3

import math
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return (xtile, ytile)

def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

from math import sin, cos, acos, radians
earth_rad = 6378.137
def latlng_to_xyz(lat, lng):
    rlat, rlng = radians(lat), radians(lng)
    coslat = cos(rlat)
    return coslat*cos(rlng), coslat*sin(rlng), sin(rlat)
def dist_on_sphere(pos0, pos1, radious=earth_rad):
    xyz0, xyz1 = latlng_to_xyz(*pos0), latlng_to_xyz(*pos1)
    return acos(sum(x * y for x, y in zip(xyz0, xyz1)))*radious

def search_and_sort_places_by_distance(lat, lon, targets):
    results = []
    for row in targets:
        dep = (lat, lon)
        dest = (float(row['latitude']), float(row['longtitude']))
        distance = dist_on_sphere(dep, dest)
        row['distance'] = distance
        results.append(row)
    list_distances = sorted(results, key=lambda x:x['distance'])
    return list_distances

import requests
import json
import random

ROUTE_URL_TEMPLATE1 = 'http://localhost:5000/route/v1/driving/{start_long},{start_lat};{end_long},{end_lat}?alternatives=true&steps=true'
ROUTE_URL_TEMPLATE2 = 'http://192.168.11.49:5000/route/v1/driving/{start_long},{start_lat};{end_long},{end_lat}?alternatives=true&steps=true'

def search_and_sort_places_by_duration(lat, lon, targets):
    route_urls = [ROUTE_URL_TEMPLATE1] + [ROUTE_URL_TEMPLATE2]*2 # 負荷分散
    results = []
    for i, row in enumerate(targets):
        server = random.choice(route_urls)
        route_url = server.format(**{'start_lat':lat, 'start_long':lon ,'end_lat':row['latitude'], 'end_long':row['longtitude']})
        r = requests.get(route_url)
        route_data =  json.loads(r.content)
        if route_data['code'] == 'Ok':
            duration = route_data['routes'][0]['duration']
        else:
            duration = None
        row['duration'] = duration
        results.append(row)
    list_durations = sorted(results, key=lambda x:x['duration'])
    return list_durations

# -------------------------------------------

# 北海道全域
EDGE_NW = (45.80000, 139.05524)
EDGE_SE = (41.23064, 146.16192)
ZOOM = 10
QUADKEY_LEVEL = 16
IMG_X , IMG_Y = (64, 64) # バッチサイズを小さくするために 4px で描画 (実質 zoom = 8)
TILE_SIZE = 256

# -------------------------------------------

import T00PopulationIndex

class generateDistanceEachTiles(luigi.Task):
    zoom = luigi.Parameter()
    x = luigi.Parameter()
    y = luigi.Parameter()
    target_name = luigi.Parameter()
    hospitals = luigi.ListParameter() # 病院名簿
    distance_threshold = luigi.FloatParameter(default=300.0) #300km以内の病院のみ
    def requires(self):
        return T00PopulationIndex.T00mainTask()
    def output(self):
        combination = {'zoom':self.zoom,'x':self.x,'y':self.y,'target':self.target_name}
        pkl_file = './var/tmp_N02_{target}_{zoom}_{x}_{y}.pkl'.format(**combination)
        return luigi.LocalTarget(pkl_file)
    def run(self):
        tile_x = self.x
        tile_y = self.y
        zoom = self.zoom
        size_x, size_y = (IMG_X, IMG_Y)
        target_list = self.hospitals

        conn = sqlite3.connect(self.input().fn)
        cur = conn.cursor()

        result_data = {"zoom":zoom, "tile_x":tile_x, "tile_y":tile_y, "distance_straight_line":[[{} for i in range(size_x)] for j in range(size_y)], "distance_path":[[{} for i in range(size_x)] for j in range(size_y)], "duration":[[{} for i in range(size_x)] for j in range(size_y)]}
        for img_y in range(0, size_y):
            for img_x in range(0, size_x):
                print "{} : {}, {} / {}, {}".format(self.output().fn, img_x, img_y, size_x, size_y)
                # 1ピクセル分
                current_pos = num2deg(tile_x + float(img_x)/size_x, tile_y + float(img_y)/size_y, zoom)

                ## 人口メッシュにデータが有る場合のみ続行する
                qkey = quadkey.from_geo(current_pos, QUADKEY_LEVEL).key
                cur.execute('select qkey from population_mesh where qkey = ?', (qkey,))
                if cur.fetchone() == None:
                    continue

                result = search_and_sort_places_by_distance(*(current_pos+(target_list,)))
                result_data["distance_straight_line"][img_y][img_x] = [{'id':x['id'],'value':x['distance']} for x in result]
                result = search_and_sort_places_by_duration(*(current_pos+([x for x in target_list if x['distance'] <= self.distance_threshold],))) # 近いものに絞って時間距離を求める
                result_data["duration"][img_y][img_x] = [{'id':x['id'],'value':x['duration']} for x in result]
                result_data["distance_path"][img_y][img_x] = [{'id':x['id'],'value':x['distance']} for x in result]
        with self.output().open('w') as f:
            pickle.dump(result_data, f, pickle.HIGHEST_PROTOCOL)

        conn.close()

class generateDb(luigi.Task):
    target_name = luigi.Parameter()
    output_db = luigi.Parameter(default="./var/T02_hospital_distance_map.db")
    hospitals = luigi.ListParameter()
    def requires(self):
        zoom = ZOOM
        edge_nw_x, edge_nw_y = deg2num( *(EDGE_NW+(zoom,)) )
        edge_se_x, edge_se_y = deg2num( *(EDGE_SE+(zoom,)) )
        print deg2num( *(EDGE_NW+(zoom,)) ) +  deg2num( *(EDGE_SE+(zoom,)) )
        tasks = []
        for tile_x in range(edge_nw_x, edge_se_x+1):
            for tile_y in range(edge_nw_y, edge_se_y+1):
                tasks.append(generateDistanceEachTiles(x=tile_x, y=tile_y, zoom=zoom, target_name=self.target_name, hospitals=self.hospitals))
        return tasks
    def output(self):
        return luigi.LocalTarget(self.output_db)
    def run(self):
        size_x, size_y = (IMG_X, IMG_Y)

        conn = sqlite3.connect(self.output().fn)
        cur = conn.cursor()
        ddl = """CREATE TABLE IF NOT EXISTS hospitals_{}(
            qkey TEXT PRIMARY KEY,
            latitude FLOAT,
            longtitude FLOAT,
            hospital_id INTEGER,
            duration FLOAT,
            distance_path FLOAT,
            distance_straight_line FLOAT
            )""".format(self.target_name)
        cur.execute(ddl)

        dml = """INSERT OR IGNORE INTO hospitals_{}(
                'qkey',
                'latitude',
                'longtitude',
                'hospital_id',
                'duration',
                'distance_path',
                'distance_straight_line')
                VALUES (?, ?, ?, ?, ?, ?, ?)""".format(self.target_name)

        for tile_data_file in self.input():
            with open(tile_data_file.fn, 'r') as f:
                result_data = pickle.load(f)

            tile_x = result_data["tile_x"]
            tile_y = result_data["tile_y"]
            zoom = result_data["zoom"]
            for img_y in range(0, size_y):
                for img_x in range(0, size_x):
                    lat, lon = num2deg(tile_x + float(img_x)/size_x, tile_y + float(img_y)/size_y, zoom)
                    # 最短の所要時間
                    try:
                        duration = result_data["duration"][img_y][img_x][0]['value']
                        distance_path = result_data["distance_path"][img_y][img_x][0]['value']
                        hospital_id = result_data["duration"][img_y][img_x][0]['id']
                        distance_straight_line = result_data["distance_straight_line"][img_y][img_x][0]['value'] # 割り切り(本当はOSRMの結果から導出したい)
                    except:
                        duration = None
                        distance_path = None
                        hospital_id = None
                        distance_straight_line = None
                    if hospital_id == None:
                        continue
                    qkey = quadkey.from_geo((lat,lon), QUADKEY_LEVEL).key
                    cur.execute(dml, (qkey, lat, lon, hospital_id, duration, distance_path, distance_straight_line))

        conn.commit()
        conn.close()
            

class mainTask(luigi.WrapperTask):
    hospital_list_csv = luigi.Parameter(default='./data/D02_hospital/hospital_list.csv')
    def requires(self):
        df_hospital = pandas.read_csv(self.hospital_list_csv)
        target_lists = {}

        #target_lists["all"] = []
        # リストに代入
        #for i, row in df_hospital.iterrows():
        #    target_lists["all"].append(row.T.to_dict())

        # 検索
        #target_lists["matanity"] = []
        #for i, row in df_hospital.iterrows():
        #    if row['services'].find('産') >= 0:
        #        target_lists["matanity"].append(row.T.to_dict())

        target_lists["matanity_delivery"] = []
        for i, row in df_hospital.iterrows():
            if row['matanity_delivery'] == '○':
                target_lists["matanity_delivery"].append(row.T.to_dict())

        target_lists["brain"] = []
        for i, row in df_hospital.iterrows():
            if row['services'].find('脳') >= 0:
                target_lists["brain"].append(row.T.to_dict())

        # タスク生成
        tasks = []
        for k,v in target_lists.items():
            task = generateDb(target_name=k, output_db="./var/T02_hospital_distance_map_{}.db".format(k),hospitals=v)
            tasks.append(task)

        return tasks

if __name__ == "__main__":
    luigi.run(['mainTask', '--workers=12', '--local-scheduler'])
