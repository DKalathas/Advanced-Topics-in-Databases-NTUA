# Advanced Topics in Databases 2022-2023 NTUA

## Installation

First of all, you must have a local network with 2 or more vms connected.

To install Hadoop and Spark follow the steps below:

First, install Hadoop follow the link -> [Hadoop](https://sparkbyexamples.com/hadoop/apache-hadoop-installation/)

Then, set up Yarn Cluster follow the link -> [Yarn](https://sparkbyexamples.com/hadoop/yarn-setup-and-run-map-reduce-program/)

Finally, install Spark with pdf adove or follow the link -> [Spark](https://sparkbyexamples.com/spark/spark-setup-on-hadoop-yarn/)

## How to execute the program (with 2 workers)

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
3.Upload data in Hadoop Distributed File System (HDFS), according to the example below:

```bash
hadoop fs -put ./yellow_tripdata_2022-01.parquet hdfs://master:9000/par/yellow_tripdata_2022-01.parquet
```
4.Start a worker in master:

```bash
start-worker.sh spark://192.168.0.2:7077
```
5.Start a worker in slave by typing the following instructions in the master vm:

```bash
ssh (slave vm connection string)
```
```bash
start-worker.sh spark://192.168.0.2:7077
```

6.Submit the task in Spark environment (in the master vm and in the directory of the file):

```bash
spark-submit (filename)
```

7.Results

See the results in the terminal 

### Team members:

Dimitris Kalathas, Dimitris Bakalis

### The assignment is in greek.
