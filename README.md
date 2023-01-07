# Project: Data Warehouse

Project submission for Udacity Data Engineering Nanodegree - Data Warehouse

## Summary
In this project we combine users song play data & metadata to augment analytics requirement. Main approach is to create an Amazon Redshift Cluster using available SDK. Secondly build an ETL data pipeline in python involving SQL to load data from log files into staging tables. And then eventually load data into schema designed for analytics. JSON data is copied from S3 buckets to Redshift staging tables(using distribution & sortkeys) before getting inserted into start schema(with fact and dimension tables). Start schema was appropriate since due to denormalized tables made analytics query easy.


## Install
```bash
$ pip install -r Requirements.txt
```

## Files

**`Manage_RS_Cluster.py`**

* Manage Redshift Cluster - Create IAM role, Redshift cluster, and allow TCP connection from outside VPC
* Pass `--delete` flag to delete resources
* Pass `--print`  flag to display DW endpoint & DW Role ARN. 
* Function `get_Cluster_Props` used to get cluster properties

**`create_tables.py`** Drop and recreate tables

**`dwh.cfg`** Configure Redshift cluster and data import

**`etl.py`** Copy data to staging tables and insert into star schema fact and dimension tables

**`sql_queries.py`**

* Creating and dropping staging and star schema tables
* Copy JSON data from S3 to Redshift staging tables
* Insert data from staging tables to star schema fact and dimension tables
