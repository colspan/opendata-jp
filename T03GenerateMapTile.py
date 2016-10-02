#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import copy
import luigi
import sqlite3
import quadkey

from T02GenerateDistanceMap import deg2num, num2deg, EDGE_NW, EDGE_SE, ZOOM, QUADKEY_LEVEL, IMG_X, IMG_Y, TILE_SIZE

from PIL import Image, ImageDraw, ImageFont

##########################
#  6200 :  60 deg, 1.0, 0.5 # yellow
#        : 120 deg, 1.0, 0.5 # green
#        : 180 deg, 1.0, 0.5 # cyan
#        : 240 deg, 1.0, 0.5 # blue
#      1 : 300 deg, 1.0, 0.5 # magenta
#      0 : 300 deg, 1.0, 0.0 # transparent
# Opacityは30分から120分まで線形で消えていく
from colorsys import hls_to_rgb
def get_color_population(value):
    hsl_hue = 300 - 240 * float(value) / 6200.0
    if hsl_hue > 300:
        # 超えていたら300になおす
        hsl_hue = 300
    if hsl_hue < 0:
        hsl_hue = 0
    if value == 0: # 0なら透明
        opacity_ratio = 0.0
    elif value < 1.0:
        opacity_ratio = value - int(value)
    else:
        opacity_ratio = 1.0
    color = tuple([int(255*x) for x in list(hls_to_rgb(float(hsl_hue)/360, 0.5, 1.0))]) + (int(opacity_ratio*255),)
    return color

#   0分 :  60 deg, 1.0, 0.5 # yellow
#  15分 : 120 deg, 1.0, 0.5 # green
#  30分 : 180 deg, 1.0, 0.5 # cyan
#  45分 : 240 deg, 1.0, 0.5 # blue
#  60分 : 300 deg, 1.0, 0.5 # magenta
# 120分 : 300 deg, 1.0, 0.0 # black
# Opacityは30分から120分まで線形で消えていく
def get_color(value):
    hsl_hue = float(value)/3600*(300-60)+60
    if hsl_hue > 300:
        # 超えていたら300になおす
        hsl_hue = 300
    if value > 1800: # 30分を超えていたら120分まで線形で消えていく
        opacity_ratio = 1.0 - float(value-1800)/5400
        if opacity_ratio < 0:
            # 0を下回っていたら0にする
            opacity_ratio = 0.0
    else:
        opacity_ratio = 1.0
    color = tuple([int(255*x) for x in list(hls_to_rgb(float(hsl_hue)/360, 0.5*opacity_ratio, 1.0))]) + (int(opacity_ratio*255),)
    return color

class generateTileImage(luigi.Task):
    zoom = luigi.Parameter()
    x = luigi.Parameter()
    y = luigi.Parameter()
    img_file = luigi.Parameter()
    database = luigi.Parameter()
    target_name = luigi.Parameter()
    query = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(self.img_file)
    def run(self):
        tile_x = self.x
        tile_y = self.y
        zoom = self.zoom
        size_x, size_y = (IMG_X, IMG_Y)

        img = Image.new('RGBA', (size_x, size_y), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)

        # raw_data/generate_geohashindex.py で作成したDB
        conn = sqlite3.connect(self.database, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("PRAGMA case_sensitive_like=ON;")

        for img_y in range(0, size_y):
            for img_x in range(0, size_x):
                # 1ピクセル分
                current_pos = num2deg(tile_x + float(img_x)/size_x, tile_y + float(img_y)/size_y, zoom)
                qkey = quadkey.from_geo(current_pos, 16).key
                #print qkey
                try:
                    c = cur.execute(self.query.format(qkey))
                    row = c.fetchone()
                    if self.target_name == "population":
                        print (row['qkey'], row['population'])
                        value = row['population']
                    else:
                        print (row['qkey'], row['duration'])
                        value = row['duration']
                except:
                    if self.target_name == "population":
                        value = 0
                    else:
                        value = 9999999999
                if self.target_name == "population":
                    color = get_color_population(value)
                else:
                    color = get_color(value)
                draw.rectangle(((img_x,img_y),(img_x+1,img_y+1)),fill=color)
            print "{} : {} / {}".format(self.img_file, img_y, size_y)
        img = img.resize((TILE_SIZE, TILE_SIZE))
        img.save(self.output().fn, 'PNG')
        conn.close()

class scheduleTileTasks(luigi.WrapperTask):
    target_name = luigi.Parameter()
    database = luigi.Parameter()
    query = luigi.Parameter()
    def requires(self):
        zoom = ZOOM
        edge_nw_x, edge_nw_y = deg2num( *(EDGE_NW+(zoom,)) )
        edge_se_x, edge_se_y = deg2num( *(EDGE_SE+(zoom,)) )
        print deg2num( *(EDGE_NW+(zoom,)) ) +  deg2num( *(EDGE_SE+(zoom,)) )

        for tile_x in range(edge_nw_x, edge_se_x+1):
            for tile_y in range(edge_nw_y, edge_se_y+1):
                # タイル1枚分
                combination = {'zoom':zoom,'x':tile_x,'y':tile_y}
                img_file = './var/tile_' + self.target_name + '/{zoom}/{x}/{y}.png'.format(**combination)
                print img_file
                # ディレクトリ作成
                dir_name = os.path.dirname(img_file)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                yield generateTileImage(x=tile_x, y=tile_y, zoom=zoom, img_file=img_file, database=self.database, target_name=self.target_name, query=self.query)

class mainTask(luigi.WrapperTask):
    def requires(self):
        #yield scheduleTileTasks(database="./var/T00_population_mesh_third_half_mesh.db", target_name="population", query=u"SELECT * FROM population_mesh WHERE qkey = '{}'")
        yield scheduleTileTasks(database="./var/T02_airport_distance_map.db", target_name="airport", query=u"SELECT * FROM airports WHERE qkey = '{}'")

if __name__ == "__main__":
    luigi.run()#['mainTask', '--workers=5'])
