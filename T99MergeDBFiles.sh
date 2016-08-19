#!/bin/sh

files=$(ls var/T*db)
dump_file='./var/T99_dumped.sql'
output_file='./var/T99_merged.db'

echo "" > $dump_file
for file in $files; do
  sqlite3 $file .dump >> $dump_file 
done

rm -f $output_file
sqlite3 $output_file < $dump_file
