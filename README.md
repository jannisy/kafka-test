# Kafka Test
This is a small Java program created for a the seminar "Hot Topics in Information System Engineering" at TU Berlin during the winter semester 2015/16. It is designed to put load on an Apache Kafka cluster. The load consists of a constant stream of read and write requests The program records the total amount of data read and written and outputs the resulting throughput rates.

## Using the application
The code to start the application is in the ThroughputTest class. Run

`ThroughputTest serverName executionTime producers consumers producerThreads`

with the following arguments:

* serverName: The host name / ip address of the Kafka broker running the Zookeeper server
* executionTime: the execution time in seconds
* producers: the number of producers created by the application
* consumers: the number of consumers created by the application
* producerThreads: the number of threads which are used to run the producers

You can also use maven's `mvn package` to build a jar file of the application.

After execution, the application will print statistics of the test to the console, such as the read and write throughput rates and the amount of data and messages transmitted.
