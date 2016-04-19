Hawk/Eagle-beta 
===============

What is it?
-----------

### Hawk

Hawk is a Hybrid Data Center Scheduler presented at [Usenix ATC 2015](https://www.usenix.org/conference/atc15/technical-session/presentation/delgado)

It takes the best of both worlds combining centralized and distributed schedulers. It has the following main features:

1. Hybrid Scheduling. Schedules Long jobs in a centralized way (better scheduling decisions) and Short jobs in a distributed way (better scheduling latency).

2. Work stealing. To do better load balance when a node is free it will contact another one and 'steal' the short-latency-sensitive jobs in the queue.

3. Partitioning. It prevents Long jobs from taking all the resources in the cluster so that Short jobs do not experience head-of-line blocking.

### Eagle

Eagle is currently work in progress. A beta version is available here.
Eagle aims to avoid the Head-of-Line blocking that short jobs experience in distributed schedulers by providing and approximate/fast view of the Long jobs.

Installation
------------

In order to run Hawk and Eagle you need to have Java JDK 1.7 installed and [Maven](https://maven.apache.org/download.cgi).

This command installs Hawk/Eagle locally.

    $ mvn install -DskipTests 


Getting started
---------------

Create a configuration file with the following parameters.

### Hawk

    deployment.mode = configbased		# currently only this mode is supported
    static.node_monitors =<hostname_1>:20502	# comma sepparated list of nodes where jobs will run
    static.app.name = spark			# the application name, this can also be changed as a java opt
    system.memory =10240000			# 
    system.cpus=1				# currently only one slot per machine is supported
    sample.ratio=2				# number of probes per task for distributed schedulers, 'power of two'
    cancellation=no				# after a job finishes will cancel the rest of the probes, in practice makes no difference
    scheduler.centralized=<centralized_scheduler_ip>	# centralized scheduler IP, if no centralized scheduler set 0.0.0.0
    big.partition=80				# the percentage of nodes where Long jobs can run
    small.partition=100				# the percentage of nodes where Short jobs can run
    nodemonitor.stealing=yes			# enable Hawk stealing
    nodemonitor.stealing_attempts=10		# number of stealing attempts
    eagle.piggybacking=no			# enable Eagle
    eagle.retry_rounds=0			# number of rounds distributed schedulers should try before going to small partition


### Eagle-beta

To enable Eagle you need to change the following parameters

    nodemonitor.stealing=no			# enable Hawk stealing
    nodemonitor.stealing_attempts=0		# number of stealing attempts
    eagle.piggybacking=yes			# enable Eagle
    eagle.retry_rounds=3			# number of rounds distributed schedulers should try before going to small partition

After creating the configuration file you can run Hawk/Eagle daemon with the following command (replace JAVA_DIR, EAGLE_JAR and CONF_FILE with their corresponding paths):

    $ JAVA_DIR -XX:+UseConcMarkSweepGC -verbose:gc -XX:+PrintGCTimeStamps -Xmx2046m -XX:+PrintGCDetails -cp EAGLE_JAR ch.epfl.eagle.daemon.EagleDaemon -c CONF_FILE

Now you need to run a front end application, you can test it with a Spark program for example.

Spark plugin
------------

We also have a plugin for Spark, you can find it [here](https://github.com/epfl-labos/spark/tree/eagle-beta).

You can compile it using the following command, provided you installed Eagle first.

    $ build/sbt assembly

You can run an example with JavaSleep, for that you need to create a file with the jobs sleeping time. The input file should have the following format:
 
    [Each line a job]
    Col1: job arrival time
    Col2: number of tasks in job
    Col3: estimated job runtime (we use normally the mean)
    Col4: (and as many cols as needed) the real duration of each task for the job (for the sleep)

    Example:
    570    2 2722 2722 2722 


1. Start the driver.

    $ spark/bin/spark-run -Dspark.driver.host=<driver_hostname> \
	-Dspark.driver.port=60501\
        -Dspark.scheduler=eagle \
        -Deagle.app.name=spark_<driver_hostname> \
        -Dspark.serializer=org.apache.spark.serializer.KryoSerializer \
        -Dspark.broadcast.port=33644 \
        org.apache.spark.examples.JavaSleep "eagle@$SCHEDULER:20503" 5 3 `hostname` $SMALL "<path_to_input_file>"

SMALL can take the values: "small" or "big" depending on if its the centralized or the distributed (centralized --> big)

2. Start the backends. This should run in each of the nodes

    $ spark/bin/spark-run \
	-Dspark.scheduler=eagle \
	-Dspark.master.port=7077 \
	-Dspark.hostname=<thismachine_hostname> \
	-Dspark.serializer=org.apache.spark.serializer.KryoSerializer \
	-Dspark.kryoserializer.buffer=128 \
	-Dspark.driver.host=<driver_hostname> \
	-Dspark.driver.port=60501 \
	-Deagle.app.name=spark_<driver_hostname> \
	-Dspark.httpBroadcast.uri=http://<driver_hostname>:33644 \
	-Dspark.rpc.message.maxSize=2047 \
	    org.apache.spark.scheduler.eagle.EagleExecutorBackend --driver-url spark://EagleSchedulerBackend@<driver_hostname>:60501

Simulator
------------

Hawk and Eagle are meant to improve job completion times in large clusters, to simulate with tens of thousands of nodes we used a simulator. This [simulator](https://github.com/epfl-labos/eagle/tree/master/simulation) is in Python, please refer to its README for further information.

Contact
-------
- Pamela Delgado <pamela.delgado@epfl.ch>
- Florin Dinu <florin.dinu@epfl.ch>
