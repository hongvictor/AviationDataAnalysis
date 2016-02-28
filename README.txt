
Problem Statments
Using the aviation data from 1990 to 2008 (provided by Amazon), and resolve the following questions:

Group 1
1. Rank the top 10 most popular airports by numbers of flights to/from the airport.
2. Rank the top 10 airlines by on-time arrival performance.
3. Rank the days of the week by on-time arrival performance

Group 2:
1. For each airport X, rank the top-10 carriers in decreasing order of on-time departure performance from X.
2. For each airport X, rank the top-10 airports in decreasing order of on-time departure performance from X.
3. For each source-destination pair X-Y, rank the top-10 carriers in decreasing order of on-time arrival performance at Y from X.
4. For each source-destination pair X-Y, determine the mean arrival delay (in minutes) for a flight from X to Y.
Clarification: For questions 1 and 2, find, for each airport, the top 10 carriers and destination airports from that airport with respect 
to on-time departure performance. We are not asking you to rank the overall top 10 carriers/airports. 

Group 3:
1. Does the popularity distribution of airports follow a Zipf distribution? If not, what distribution does it follow?
2. Tom wants to travel from airport X to airport Z. However, Tom also wants to stop at airport Y for some sightseeing on the way. 
More concretely, Tom has the following requirements (For specific queries, see the Task 1 Queries and Task 2 Queries):
	a. The second leg of the journey (flight Y-Z) must depart two days after the first leg (flight X-Y). 
	For example, if X-Y departs January 5, 2008, Y-Z must depart January 7, 2008.
	b. Tom wants his flights scheduled to depart airport X before 12:00 PM local time and to depart airport Y after 12:00 PM local time.
	c. Tom wants to arrive at each destination with as little delay as possible (Clarification: assume you know the actual delay of each flight).

	The mission is to find, for each X-Y-Z and day/month (dd/mm) combination in the year 2008, the two flights (X-Y and Y-Z) that satisfy 
	constraints (a) and (b) and have the best individual performance with respect to constraint (c), if such flights exist.

Notes:
1. For the queries in Group 2 and Question 3.2, you will need to compute the results for ALL input values (e.g., airport X, source-destination 
   pair X-Y, etc.) for which the result is nonempty. These results should then be stored in Cassandra so that the results for an input value 
   can be queried by a user. Then, closer to the grading deadline, we will give you sample queries (airports, flights, etc.) to include in 
   your video demo and report.

2. For example, after completing Question 2.2, a user should be able to provide an airport code (such as “ATL”) and receive the top 10 airports
   in decreasing order of on-time departure performance from ATL. Note that for questions such as 2.3, you do not need to compute the values for 
   all possible combinations of X-Y, but rather only for those such that a flight from X to Y exists.

   
Data source: 
Use Aviation data from Amazon
https://aws.amazon.com/datasets/transportation-databases/

The EBS snapshot ID for the transportation dataset is snap-e1608d88 for Linux/Unix and snap-37668b5e for Windows, 
in the us-east-1 (N. Virginia) region. You will need to launch an EC2 instance with the EBS volume attached and then mount the volume. 
For detailed instructions on how to attach an EBS volume and make it available for use, see the EBS Volumes User Guide for Linux/Unix or Windows, 
and the Using Public Data Sets Guide.

Note: a word of caution. Make sure you keep an eye on your AWS billing usage, and turn off your instances when you are done with any single 
development session. If you just disconnect and keep them running, they will keep costing you money. If you run out of credits that are given 
to you for this course, you will have to pay the rest yourself! Also, we highly suggest you develop and debug your code on fewer instances of 
wimpier nodes (e.g., medium AWS instances), and with a small sample of the whole dataset. Once you are certain your code works, ramp up to larger 
machines to process the whole dataset.

The data files are located in /aviation/airline_ontime folder


System Overview: 
1. Hadoop MapReduce
The system is essentially a 4 node Apache Hadoop cluster with one namenode and three datanodes running on AWS EC2. 
Hadoop Yarn runs and serves as the resource manager and application manager.
Cassandra cluster is deployed on the three Hadoop datanodes to provide data replication and scalability. 
The cluster run on the same rack and utilizes GossipingPropertyFileSnitch scheme. One of the nodes is assigned as the seed provider.
Source code Structure:
G1Q1/
G1Q2/
G2/
	Q1/
	Q2
	Q3/
G3Q1/
G3Q2/
Notes: requires Hadoop/HDFS, Pig, Java Cassandra installation. gnuplot is required for plotting.


2. Kafka + SparkStreaming
This distributed system is composed of a Spark cluster with one master node and three works nodes running on AWS EC2. 
The cluster enables high-throughput, fault tolerant stream processing of aviation data, which is hosted on AWS EBS. 
A Kafka cluster running with 4 brokers serves as the ingestion platform, and feeds the data to Spark Streaming services. 
There is a degree of coordination among the processes and the synchronization between the processes is maintained by Zookeeper service. 
There is also a Cassandra database cluster deployed on the three Spark worker nodes. After Spark processes the data, 
it either outputs on the screen or sends results to Cassandra cluster, which offers data replication and fault tolerance.  
The cluster run on the same rack and utilizes SimpleSnitch scheme

Source code Structure:
G1Q1/
G1Q2/
G2Q1/
G2Q2/
G2Q3/
setup/         	: requires Hadoop/HDFS and Pig installation
setup_awk/ 		: use awk. It takes longer to process data than Pig

