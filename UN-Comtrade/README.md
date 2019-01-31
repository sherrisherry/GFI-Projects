# Downloading and Preprocessing Comtrade Data

This project downloads the Comtrade datasets, downsizes them, and stores them in a completely automated manner.
I didn't use the [public API](https://comtrade.un.org/data/doc/api/) because it has limit on the number of records. I used the [bulk API](https://comtrade.un.org/data/Doc/api/bulk) instead.

## List of Files

* bulk_download.R
  * The main program
  * You may see a lot of while loops. They are used to handle errors. I planned to package them into functions in the future so later codes look cleaner.

* logs/
  * bulk_download.log: progress report of running bulk_download.R
  * download.log: information of downloaded datasets; bulk_download.R uses this file to determine which years to download and update it after downloading.

## Update History

* 01/30/2019: repository creation
