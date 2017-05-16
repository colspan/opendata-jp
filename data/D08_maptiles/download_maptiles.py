#!/usr/bin/python
# -*- coding: utf-8 -*-

from urlparse import urlparse
import os
import luigi
import requests

# 参考文献
# http://qiita.com/s-wakaba/items/f414bed3736dc5b368c8
from math import sin, cos, tan, radians, degrees, pi, log, atan, sinh
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

class DownloadTile(luigi.Task):
    """
    Wikipediaのダンプデータをダウンロードする
    """
    baseUrl = luigi.Parameter()
    baseName = luigi.Parameter()
    x = luigi.IntParameter()
    y = luigi.IntParameter()
    z = luigi.IntParameter()
    def output(self):
        extension = os.path.splitext(urlparse(self.baseUrl).path)[1]
        return luigi.LocalTarget("./var/{}/{}/{}/{}.{}".format(self.baseName, self.z,self.x, self.y, extension))
    def run(self):
        url = self.baseUrl.format(**{"x":self.x, "y":self.y, "z":self.z})
        r = requests.get(url, stream=True)
        with self.output().open("wb") as f_out:
            for chunk in r.iter_content(chunk_size=1024):
                f_out.write(chunk)

class DownloadBounds(luigi.WrapperTask):
    baseUrl = luigi.Parameter(default="http://cyberjapandata.gsi.go.jp/xyz/ort/{z}/{x}/{y}.jpg")
    baseName = luigi.Parameter(default="output")
    w = luigi.FloatParameter()
    n = luigi.FloatParameter()
    s = luigi.FloatParameter()
    e = luigi.FloatParameter()
    z = luigi.IntParameter()
    def requires(self):
        edge_nw_x, edge_nw_y, _, _ = deg_to_num( self.n, self.w, self.z )
        edge_se_x, edge_se_y, _, _ = deg_to_num( self.s, self.e, self.z )
        print deg_to_num( self.n, self.w, self.z ) +  deg_to_num( self.s, self.e, self.z )
        for tile_x in range(edge_nw_x, edge_se_x+1):
            for tile_y in range(edge_nw_y, edge_se_y+1):
                print "scheduling z:{} x:{} y:{}".format(self.z, tile_x, tile_y)
                yield DownloadTile(self.baseUrl, self.baseName, tile_x, tile_y, self.z)


if __name__ == "__main__":
    luigi.run()
