# Big Data Assigment 2

Installation and Setup
  - For standalone hadoop setup follow [this](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html)
  - To use docker setup in the project, install Docker following [this](https://docs.docker.com/install/)


# Execution using Docker
  - From command line CD into project root folder (where Dockerfile is present)
  - Execute the below commands (after installing docker)
 ```sh
 $ docker build -t bdsa2 .
 $ docker run -v `pwd`:/code -p 8020:8020 -p 50070:50070 -p 50060:50060 -p 50030:50030 -p 8088:8088 -p 8080:8080 -p 19888:19888 -it bdsa2 bash
 ```
  - After you have the bash of the container run the below command
 ```sh
 $ /entrypoint.sh &
 ```

 # Building the jar file
 ```sh
 $ ./gradlew build
 ```
 **The jar file will be present in the folder build/lib

 # Downloading Dataset and copying to HDFS
 Downlaoding the dataset
 ```sh
 $ wget https://obamawhitehouse.archives.gov/files/disclosures/visitors/WhiteHouse-WAVES-Released-1210.zip
 $ unzip WhiteHouse-WAVES-Released-1210.zip
 ```
 Loading data into HDFS
 ```sh
 $ hdfs dfs -mkdir /input
 $ hdfs dfs -copyFromLocal WhiteHouse-WAVES-Released-1210.csv /input
 ```

# Executing MapReduce tasks using the jar file
For counting frequency and get top N
```sh
$ hadoop jar visitoranalysis-1.0-SNAPSHOT.jar com.bdsassignment.GroupByCountAndLimit <input_file_path> <output_file_path> <column_indices_separated_by_comma> <top_n_value> <number_of_reducers>
```
Example command for executing getting top 10 frequently visited people using 4 reducers
```sh
hadoop jar visitoranalysis-1.0-SNAPSHOT.jar com.bdsassingment.GroupByCountAndLimit /input /top10visitors 0,1,2 10 4
```

For counting frequency
```sh
$ hadoop jar visitoranalysis-1.0-SNAPSHOT.jar com.bdsassingment.GroupByCount <input_file_path> <output_file_path> <column_indices_separated_by_comma>
```

For fetching top N when input data is in the format ```word\tcount```
```sh
$ hadoop jar visitoranalysis-1.0-SNAPSHOT.jar com.bdsassingment.TopNDriver <input_file_path> <output_file_path> <top_n>
```
 