# GFI-Cloud

This repository is intended to be a mirror of the home folder of GFI's data analyst on AWS, which may include multiple projects. The reason for using this structure is that it makes clear of what files are shared between projects.

Some files are modified to prevent leaking sensitive information such as our user credentials.

At this stage, we use a data lake rather than a database strategy to manage data so that we can be flexible with our analysis.

## System Environment

* Computing: AWS EC2
  * Operation System: Ubuntu Linux
  * Type: t2.micro

* Memory: AWS EBS
  * 9G root, gp2
  * 12G - 21G swap, gp2

* Storage: AWS S3 + EFS
  * S3
    * gfi-comtrade: downsized Comtrade datasets
    * gfi-supplemental: supplemental files
    * gfi-mirror-analysis: paired data for mirror analysis
	* gfi-work: intermediate results that used repeatly
	* gfi-archive: backup files
  * EFS
    * mounted to /efs
	* /efs/work: results of each project

## Structure of Repository

* vars/: Files shared by multiple projects, usually user credentials and environment settings.

* UN-Comtrade/: Dealing with Comtrade data

* WB-WITS/: Dealing with World Bank WITS data

* pkg/: package for GFI projects

* norm/: normalizing legacy data for new process

* documentation.docx: documentation of all the projects; refer to the master flow chart in each project folder for detailed process in a technical aspect.

* spacious.sh: environment setup for processes that run in a single computing unit.

* spark-master.sh: environment setup of the master node for processes that run on multiple computing units.

* spark-worker.sh: environment setup of the worker nodes for processes that run on multiple computing units.

* .Renviron: environment setting for R sessions; complementing the *.sh environment setup files.

## Update History

* 01/30/2019: repository creation, with vars/ and UN-Comtrade/
* 02/19/2019: debuging, improving efficiency and robustness
* 03/10/2019: implemented distributed computing
