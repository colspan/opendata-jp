#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import json
import sqlite3
import quadkey

import numpy as np
import scipy.ndimage

# 参考文献
# http://qiita.com/s-wakaba/items/f414bed3736dc5b368c8
from math import sin, cos, tan, acos, asin, atan2, radians, degrees, pi, log


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



# ==============

import luigi
class T00mainTask(luigi.Task):
    input_db = luigi.Parameter(default="./var/T02_airport_distance_map.db")
    output_file = luigi.Parameter(default="./var/U00_airport.json")
    def output(self):
        return luigi.LocalTarget(self.output_file)
    def run(self):
        # DB準備
        features = []

        conn = sqlite3.connect(self.input_db)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        row = cur.execute(u"select * from airports")
        i = 0
        for row in cur:
            tile_info = quadkey.QuadKey(row["qkey"]).to_tile()
            level = tile_info[1]
            north_west_px = tile_info[0]
            south_east_px = (north_west_px[0]+1,north_west_px[1]+1)

            nw_geo = quadkey.from_tile(north_west_px, level).to_geo()
            se_geo = quadkey.from_tile(south_east_px, level).to_geo()

            if False:
                print quadkey.QuadKey(row["qkey"]).to_geo()
                print nw_geo
                print se_geo
                print row["population"]

            coordinates = [
                [nw_geo[1], nw_geo[0]],
                [se_geo[1], nw_geo[0]],
                [se_geo[1], se_geo[0]],
                [nw_geo[1], se_geo[0]],
                [nw_geo[1], nw_geo[0]]
            ]

            feature = {
                "type":"Feature",
                "geometry":{"type":"MultiPolygon",
                    "coordinates":[
                        [coordinates]
                    ]
                },
                "properties":{"cartodb_id":i,"value":row["duration"],"airport_id":row['hospital_id']}
            }
            features.append(feature)
            i+=1


        geojson_obj = {
            "type":"FeatureCollection",
            "features":features
        }

        with self.output().open("w") as f:
            json.dump(geojson_obj, f)

        conn.close()

if __name__ == "__main__":
    luigi.run(['T00mainTask', '--workers=1', '--local-scheduler'])

"""
{"type": "FeatureCollection",
 "features": [
	{"type":"Feature",
		"geometry":{"type":"MultiPolygon",
			"coordinates":[
				[
					[
						[142.325662,44.465215],[142.325672,44.465665],[142.326301,44.465658],[142.326291,44.465208],[142.325662,44.465215]
					]
				]
			]
		},
		"properties":{"cartodb_id":9074,"value":24,"label":null}
	}
 ]
}

"""
