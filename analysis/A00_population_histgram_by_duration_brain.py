#!/usr/bin/python
# -*- coding: utf-8 -*-

import sqlite3

db_file = "../var/T99_merged.db"
conn = sqlite3.connect(db_file)
cur = conn.cursor()

query = "select count(1), sum(p.value), avg(m.duration) from hospitals_brain as m inner join mesh_population as p on p.qkey = m.qkey where m.duration < ? and m.duration >= ? and m.ranking = 0;"


step = 5
for duration in range(0,360,step):
    row = cur.execute(query, ((duration+step)*60, duration*60))
    for row in cur:
        print "{},{}".format(duration, row[1])

conn.commit()
conn.close()
