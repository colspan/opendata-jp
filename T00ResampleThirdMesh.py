#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import json
import quadkey

import numpy as np
import scipy.ndimage

# 参考文献
# http://qiita.com/s-wakaba/items/f414bed3736dc5b368c8
from math import sin, cos, tan, acos, asin, atan2, radians, degrees, pi, log

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

def mesh_code_to_latlng(mesh_code):
    if len(mesh_code) == 9:
        # 1/2メッシュコード
        half_lat = float((int(mesh_code[8])-1)/2) * 1.0/8.0/10.0/2.0/1.5 
        half_lon = float((int(mesh_code[8])-1)%2) * 1.0/8.0/10.0/2.0 
    else:
        # 3次メッシュコード
        half_lat = 0
        half_lon = 0
    latitude   = float(mesh_code[0:2])/1.5 + float(mesh_code[4])*1.0/8.0/1.5 + float(mesh_code[6])*1.0/8.0/10.0/1.5 + half_lat
    longtitude = float(mesh_code[2:4])+100.0 + float(mesh_code[5])*1.0/8.0 + float(mesh_code[7])*1.0/8.0/10.0 + half_lon
    return (latitude, longtitude)

def mesh_code_to_latlng_index(mesh_code):
    if len(mesh_code) == 9:
        # 1/2メッシュコード
        half_lat = (int(mesh_code[8])-1)/2 
        half_lon = (int(mesh_code[8])-1)%2
    else:
        # 3次メッシュコード
        half_lat = 0
        half_lon = 0
    latitude = int(mesh_code[0:2])*8*10*2 + int(mesh_code[4])*10*2 + int(mesh_code[6])*2 + half_lat
    longtitude = int(mesh_code[2:4])*8*10*2 + int(mesh_code[5])*10*2 + int(mesh_code[7])*2 + half_lon
    return (latitude, longtitude)



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


from PIL import Image, ImageDraw, ImageFont
def draw_matrix(input_array, filename):
    img = Image.new('RGBA', input_array.shape, (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    max_value = input_array.max()
    for y, row in enumerate(input_array):
        for x, value in enumerate(row):
            c = int( 255 * value / max_value ) * 30
            draw.rectangle( ((x,y),(x+1,y+1)), fill=(c,c,c,255) )
    img.save(filename, 'PNG')
    

# ==============

import luigi
from luigi.contrib import sqla
from sqlalchemy.types import *
import re
class resampleData(sqla.CopyToTable):
    mesh_data = luigi.Parameter(default="./data/D00_population/population_mesh_third_half.csv")
    output_db = luigi.Parameter(default="./var/T00_third_half_mesh.db")
    table_name = luigi.Parameter(default="mesh_population")
    value_row_number = luigi.IntParameter(default=1)
    no_unit_value = luigi.BoolParameter(default=False)
    def __init__(self, *args, **kwargs):
        super(sqla.CopyToTable, self).__init__(*args, **kwargs)
        self.columns = [
            (["qkey", String(32)], {"index": True}),
            (["latitude", Float()], {}),
            (["longtitude", Float()], {}),
            (["value", Float()], {}),
        ]

    @property
    def connection_string(self):
        return "sqlite:///{}".format(self.output_db)

    @property
    def table(self):
        return self.table_name

    def rows(self):
        # 3次メッシュデータ読み込み
        r = re.compile(r"[0-9]{8}")
        stat_third_mesh = {}
        with open(self.mesh_data, "r") as f:
            for i,line in enumerate(f):
                row = line.rstrip().split(',')
                mesh_code = row[0]
                if not r.match(mesh_code):
                    continue
                value = float(row[self.value_row_number])
                lat, lon = mesh_code_to_latlng(mesh_code)
                stat_third_mesh[mesh_code] = [mesh_code, value, lat, lon]

        # 緯度経度の秒数を求め、それぞれ最大最小を求める
        area_lat_max = max(stat_third_mesh.values(), key=(lambda x : x[2]))[2]
        area_lat_min = min(stat_third_mesh.values(), key=(lambda x : x[2]))[2]
        area_lon_max = max(stat_third_mesh.values(), key=(lambda x : x[3]))[3]
        area_lon_min = min(stat_third_mesh.values(), key=(lambda x : x[3]))[3]
        lat_index_min = mesh_code_to_latlng_index( min(stat_third_mesh.values(), key=(lambda x : x[2]))[0] )[0]
        lon_index_min = mesh_code_to_latlng_index( min(stat_third_mesh.values(), key=(lambda x : x[3]))[0] )[1]
        #print (area_lat_max,area_lat_min,area_lon_max,area_lon_min)

        # 位置計算関数作成 (度から秒に換算した上で割る, 1/2メッシュなので2を掛ける)
        get_index_lat = lambda x : int((x - area_lat_min)*3600.0*2.0/30.0 + 1.0) # quadkeyの視点は北西、1/2メッシュの視点は南西なのでインデックスがずれる
        get_index_lon = lambda x : int((x - area_lon_min)*3600.0*2.0/45.0 )

        # メッシュ数計算
        mesh_nums = (get_index_lat(area_lat_max)+1, get_index_lon(area_lon_max)+1)
        #print mesh_nums

        # 統計値格納配列を準備する
        array_third_mesh = np.zeros(mesh_nums[0]*mesh_nums[1]).reshape(mesh_nums[0],mesh_nums[1])

        # 統計値代入
        for x in stat_third_mesh.values():
            current_mesh_code = x[0]
            lat_index , lon_index = mesh_code_to_latlng_index(x[0])
            array_third_mesh[lat_index - lat_index_min, lon_index - lon_index_min] = x[1]
            if(len(current_mesh_code) == 8):
                array_third_mesh[lat_index - lat_index_min - 1, lon_index - lon_index_min] = x[1]
                array_third_mesh[lat_index - lat_index_min, lon_index - lon_index_min - 1] = x[1]
                array_third_mesh[lat_index - lat_index_min - 1, lon_index - lon_index_min - 1] = x[1]

        draw_matrix(array_third_mesh, './var/T00_mesh_code_map_{}.png'.format(self.table_name) )

        # quadkey空間におけるピクセル数算出
        zoom = 8
        pixel_ne = deg_to_pixel_coordinates(area_lat_max, area_lon_max, zoom)
        pixel_sw = deg_to_pixel_coordinates(area_lat_min, area_lon_min, zoom)
        pixel_nums = (pixel_sw[1] - pixel_ne[1] + 1, pixel_ne[0] - pixel_sw[0] + 1) # h*w
        print pixel_nums
        # リサンプリング比率計算
        resampling_ratio = (float(pixel_nums[0])/float(mesh_nums[0]-1),float(pixel_nums[1])/float(mesh_nums[1]-1))
        print resampling_ratio

        # リサンプリング実行
        print array_third_mesh.sum()
        resampled_mesh = scipy.ndimage.zoom(array_third_mesh, resampling_ratio, order=0)
        # ゴミをゼロに丸める
        value_limit = 0.001 # 0.01 / resampling_ratio[0] / resampling_ratio[1]
        #resampled_mesh = resampled_mesh * (resampled_mesh < value_limit)

        if self.no_unit_value:
            # 単位のない値(割合等)は値を正規化しない
            quadkey_mesh = resampled_mesh
        else:
            # 値を正規化する
            quadkey_mesh = resampled_mesh * array_third_mesh.sum() / resampled_mesh.sum()

        # リサンプル後のメッシュデータのインデックスから緯度経度を計算する関数
        index_to_deg = lambda i,j: (float(i)/pixel_nums[0]*(area_lat_max - area_lat_min)+area_lat_min, float(j)/pixel_nums[1]*(area_lon_max - area_lon_min)+area_lon_min)

        sum_value = 0
        # リサンプル後のメッシュデータの各要素をループしてDBに保存
        qkeys = []
        for lat_index, row in enumerate(quadkey_mesh):
            for lon_index, value in enumerate(row):
                if value < value_limit:
                    continue
                else:
                    sum_value += value
                lat, lon = index_to_deg(lat_index, lon_index)
                qkey =  quadkey.from_geo((lat,lon), 16).key
                exists = qkey in qkeys
                qkeys.append(qkey)
                try:
                    yield (qkey, lat, lon, value)
                except:
                    continue
        print sum_value

class T00mainTask(luigi.WrapperTask):
    def requires(self):
        tasks = [
            resampleData(
                mesh_data = "./data/D00_population/population_mesh_third_half.csv",
                output_db = "./var/T00_third_half_mesh.db",
                table_name = "mesh_population",
                value_row_number = 1,
                no_unit_value = False
            ),
            resampleData(
                mesh_data = "./data/D00_population/population_mesh_third_half.csv",
                output_db = "./var/T00_third_half_mesh.db",
                table_name = "mesh_population_men",
                value_row_number = 2,
                no_unit_value = False
            ),
            resampleData(
                mesh_data = "./data/D00_population/population_mesh_third_half.csv",
                output_db = "./var/T00_third_half_mesh.db",
                table_name = "mesh_population_women",
                value_row_number = 3,
                no_unit_value = False
            ),
            resampleData(
                mesh_data = "./data/D07_evelation_usage/g04_hokkaido.csv",
                output_db = "./var/T00_third_half_mesh.db",
                table_name = "elevation",
                value_row_number = 1,
                no_unit_value = True
            )
        ]
        return tasks

if __name__ == "__main__":
    luigi.run(['T00mainTask', '--workers=1', '--local-scheduler'])
