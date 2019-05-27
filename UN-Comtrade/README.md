# Downloading and Preprocessing Comtrade Data

This project downloads the Comtrade datasets, downsizes them, and stores them in a completely automated manner.
I didn't use the [public API](https://comtrade.un.org/data/doc/api/) because it has limit on the number of records for download. I used the [bulk API](https://comtrade.un.org/data/Doc/api/bulk) instead.

## List of Files

* bulk_download.R
  * The main program

* logs/
  * bulk_download.log: progress report of running bulk_download.R
  * download.log: information of downloaded datasets; bulk_download.R uses this file to determine which years to download and update it after downloading.

* data/
  * process by-products.

## Update History

* 01/30/2019: folder creation
