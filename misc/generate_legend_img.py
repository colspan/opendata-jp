#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import copy
import luigi

import pickle

from PIL import Image, ImageDraw, ImageFont

from common import *
from params import *

##########################

img = Image.new('RGBA', (size_x, size_y), (0, 0, 0, 0))
draw = ImageDraw.Draw(img)

for x in range()
color = get_color(value)
draw.rectangle(((img_x,img_y),(img_x+1,img_y+1)),fill=color)

img = img.resize((TILE_SIZE, TILE_SIZE))
img.save(self.output().fn, 'PNG')
