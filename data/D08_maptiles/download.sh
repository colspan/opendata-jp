#!/bin/sh

# 北海道
#python download_maptiles.py DownloadBounds --DownloadBounds-baseName=hokkaido --DownloadBounds-w=139.05 --DownloadBounds-n=45.8 --DownloadBounds-s=41.23 --DownloadBounds-e=150.2 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 札幌近辺
python download_maptiles.py DownloadBounds --DownloadBounds-baseName=sapporo --DownloadBounds-w=140.78704833984375 --DownloadBounds-n=43.18715513581086 --DownloadBounds-s=42.627896481020855 --DownloadBounds-e=141.998291015625 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 十勝近辺
python download_maptiles.py DownloadBounds --DownloadBounds-baseName=tokachi --DownloadBounds-w=142.74810791015625 --DownloadBounds-n=43.25320494908846 --DownloadBounds-s=42.21224516288584 --DownloadBounds-e=143.72589111328125 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 東京
python download_maptiles.py DownloadBounds --DownloadBounds-baseName=kanto --DownloadBounds-w=139.559326171875 --DownloadBounds-n=35.77994251888403 --DownloadBounds-s=35.36217605914681 --DownloadBounds-e=140.2569580078125 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 札幌近辺
python download_maptiles.py DownloadBounds --DownloadBounds-baseUrl=http://cyberjapandata.gsi.go.jp/xyz/gazo1/{z}/{x}/{y}.jpg --DownloadBounds-baseName=1974sapporo --DownloadBounds-w=140.78704833984375 --DownloadBounds-n=43.18715513581086 --DownloadBounds-s=42.627896481020855 --DownloadBounds-e=141.998291015625 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 十勝近辺
python download_maptiles.py DownloadBounds --DownloadBounds-baseUrl=http://cyberjapandata.gsi.go.jp/xyz/gazo1/{z}/{x}/{y}.jpg --DownloadBounds-baseName=1974tokachi --DownloadBounds-w=142.74810791015625 --DownloadBounds-n=43.25320494908846 --DownloadBounds-s=42.21224516288584 --DownloadBounds-e=143.72589111328125 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 東京
python download_maptiles.py DownloadBounds --DownloadBounds-baseUrl=http://cyberjapandata.gsi.go.jp/xyz/gazo1/{z}/{x}/{y}.jpg --DownloadBounds-baseName=1974kanto --DownloadBounds-w=139.559326171875 --DownloadBounds-n=35.77994251888403 --DownloadBounds-s=35.36217605914681 --DownloadBounds-e=140.2569580078125 --DownloadBounds-z=15 --workers=4 --local-scheduler



