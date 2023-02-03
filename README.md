# Advanced Topics in Databases 2022-2023 NTUA

## Installation

Firt of all, you must have a local network with 2 or more vms connected.

For Installation Hadoop and Spark follow the steps:

First install Hadoop follow the link -> [Hadoop](https://sparkbyexamples.com/hadoop/apache-hadoop-installation/)

Second set up Yarn Cluster follow the link -> [Yarn](https://sparkbyexamples.com/hadoop/yarn-setup-and-run-map-reduce-program/)

Third install Spark with pdf adove or follow the link -> [Spark](https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/)

## How to run (with 2 workers)

1.Connect to master vm:

```bash
ssh (master vm connection string)
```
2.Start Hadoop and Spark in master vm:

```bash
start-dfs.sh
```
```bash
start-master.sh
```
3.Upload data in Hadoop Distributed File System (HDFS):

example for one

```bash
hadoop fs -put ./yellow_tripdata_2022-01.parquet hdfs://master:9000/par/yellow_tripdata_2022-01.parquet
```
4.Start a worker in master:

```bash
start-worker.sh
```
5.Start a worker in slave:

inside master vm

```bash
ssh (slave vm connection string)
```
```bash
start-worker.sh
```

6.Submit task in Spark environment:

go back in master vm in directory of python file 

```bash
spark-submit (filename)
```

7.Results

See the results in terminal 

### Team members:

Dimitris Kalathas, Dimitris Mpakalis

### The assignments language is greek.
