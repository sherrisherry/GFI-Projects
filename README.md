# GFI-Cloud

This repository is intended to be a mirror of the home folder of data analyst on AWS, which may include multiple projects. The reason for using this structure is that it makes clear of what files are shared between projects.

Some files are modified to prevent leaking sensitive information such as our user credentials.

At this stage, we use a data lake rather than a database strategy to manage data so that we can be flexible with our analysis.

## System Environment

* Computing: AWS EC2
  * Operation System: Ubuntu Linux
  * Type: t2.micro

* Memory: AWS EBS
  * 9G root, gp2
  * 21G swap, gp2

* Storage: AWS S3
  * gfi-comtrade: downsized Comtrade datasets
  * gfi-supplemental: supplemental files
  * gfi-mirror-analysis: matched data for mirror analysis

* Supplement: Google Drive
  * work progress reports

## Structure of Repository

* vars/: Files shared by multiple projects, usually user credentials and environment settings.

* UN-Comtrade/: Dealing with raw Comtrade data

## Update History

* 01/30/2019: repository creation, with vars/ and UN-Comtrade/
* 02/19/2019: large update, improving efficiency and robustness
