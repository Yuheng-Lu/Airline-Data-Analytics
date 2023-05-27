# Airline Data Analytics ✈️

## Intro

* This is a reminder APP that allows you to add your own tasks and due date.
* Double click the task to highlight.

## How to Run

* For the MapReduce programs:

  1. First, compile the Java classes:

     ```shell
     javac -classpath `hadoop classpath` *.java
     ```

  2. Second, create the JAR file:

     ```shell
     jar cvf <jobName>.jar *.class
     ```

  3. Third, put the input data file into HDFS:

     ```shell
     hadoop fs -mkdir playGround
     hadoop fs -put <airlineData>.csv playGround
     ```

  4. Forth, run the MapReduce program:

     ```shell
     hadoop jar <jobName>.jar <jobName> <airlineData>.csv lab2/output
     ```

  5. To verify that the program has run and the results are correct:

     ```shell
     hadoop fs -ls playGround/output
     hadoop fs -cat playGround/output/part-r-00000
     ```

* For the Trino commands:

  1. Start the Trino or Presto shell
  2. Select a connector that can access the data
  3. Run those queries