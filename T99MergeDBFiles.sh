#!/bin/sh

files=$(ls var/T*db)
dump_file='./var/T99_dumped.sql'
output_file='./var/T99_merged.db'

rm -f $output_file
for file in $files; do
  sqlite3 $file .dump | sqlite3 $output_file
done
