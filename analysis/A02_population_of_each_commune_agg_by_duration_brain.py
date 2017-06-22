#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
脳疾患取り扱い病院時間距離 各市町村平均最大最小中央
"""

import sqlite3
import params

db_file = "../var/T99_merged.db"
out_file = "../var/A02_population_of_each_commune_agg_by_duration_brain.csv"

conn = sqlite3.connect(db_file)
cur = conn.cursor()

query = u"select sum(p.population), avg(m.duration), max(m.duration), min(m.duration) from hospitals_brain as m inner join (select q.qkey, pm.value as population from mesh_population as pm inner join  (select * from commune_qkey as cq inner join communes as c on c.commune_id = cq.commune_id where c.commune = ?) as q on pm.qkey = q.qkey) as p on p.qkey = m.qkey where m.ranking = 0;"

def print_record(f, name, row):
    for row in cur:
        print name.encode('utf-8')
        print >> f, "{},{},{},{},{}".format(name.encode('utf-8'), row[0], row[1], row[2], row[3])

with open(out_file, "w") as f:
    print >> f, ",".join(["脳疾患取り扱い病院時間距離","人口","時間距離 平均","時間距離 最大","時間距離 最小","時間距離 最大最小比"])

    for commune in params.communes:
        row = cur.execute(query, (commune,))
        print_record(f, commune, row)

conn.close()
