#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
産婦人科時間距離 各市町村時間別分布
"""

import sqlite3
import params
import luigi

db_file = "../var/T99_merged.db"
tmp_out_file = "../var/A03_tmp_{}.csv"
out_file = "../var/A03_population_histgram_of_each_commune_agg_by_duration_matanity_delivery.csv"
target_table = "hospitals_matanity_delivery"
base_query = "select sum(p.population) from {} as m inner join (select q.qkey, population from population_mesh as pm inner join  (select * from commune_qkey as cq inner join communes as c on c.commune_id = cq.commune_id where c.commune = ?) as q on pm.qkey = q.qkey) as p on p.qkey = m.qkey".format(target_table)
population_query = "select sum(p.population) from (select q.qkey, population from population_mesh as pm inner join  (select * from commune_qkey as cq inner join communes as c on c.commune_id = cq.commune_id where c.commune = ?) as q on pm.qkey = q.qkey) as p"

DURATION_BEGIN = 0
DURATION_END = 360
DURATION_STEP = 15
duration_candidates = range(DURATION_BEGIN, DURATION_END, DURATION_STEP)

def print_record(f, name, row):
    print name
    print >> f, "{},{}".format(name, ",".join([str(x) for x in row])).replace("None","")

class calc_histgram(luigi.Task):
    commune = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(tmp_out_file.format(self.commune))
    def run(self):
        conn = sqlite3.connect(db_file)
        conn.text_factory = str
        cur = conn.cursor()
        # 人口取得
        row = cur.execute(population_query, (self.commune,))
        for row in cur:
            population = row[0]
        # 時間別人口取得
        query = base_query + " where m.duration < ? and m.duration >= ?"
        print self.commune
        durations = []
        for duration in duration_candidates:
            row = cur.execute(query, (self.commune, (duration+DURATION_STEP)*60, duration*60))
            for row in cur:
                value = row[0]
                if None == value:
                    value = 0
                durations.append(value)
                print value        
        with self.output().open("w") as f:
            print_record(f, self.commune, durations + [x/population for x in durations])
        conn.close()


class generate_csv(luigi.Task):
    def output(self):
        return luigi.LocalTarget(out_file)
    def requires(self):
        return [calc_histgram(commune=x.encode('utf-8')) for x in params.communes]
    def run(self):
        with self.output().open("w") as f:
            print >> f, ",".join(["産婦人科時間距離"] + ["人口({}～{}分)".format(x, x+DURATION_STEP) for x in duration_candidates] + ["人口率({}～{}分)".format(x, x+DURATION_STEP) for x in duration_candidates])
            for commune in self.input():
                print >> f, commune.open().read().strip()


if __name__ == "__main__":
    luigi.run(['generate_csv', '--workers=6', '--local-scheduler'])
