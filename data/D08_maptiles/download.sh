#!/bin/sh

# 北海道
python download_maptiles.py DownloadBounds --DownloadBounds-baseName=hokkaido --DownloadBounds-w=139.05 --DownloadBounds-n=45.8 --DownloadBounds-s=41.23 --DownloadBounds-e=150.2 --DownloadBounds-z=15 --workers=4 --local-scheduler

# 東京
python download_maptiles.py DownloadBounds --DownloadBounds-baseName=kanto --DownloadBounds-w=139.559326171875 --DownloadBounds-n=35.77994251888403 --DownloadBounds-s=35.36217605914681 --DownloadBounds-e=140.2569580078125 --DownloadBounds-z=15 --workers=4 --local-scheduler

