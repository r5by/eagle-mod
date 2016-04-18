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

Eagle is currently work in progress. A beta version is published here.
Eagle aims to avoid the Head-of-Line blocking that short jobs experience in distributed schedulers by providing and approximate/fast view of the Long jobs.


Installation
------------


Getting started
---------------


Contact
-------
- Pamela Delgado <pamela.delgado@epfl.ch>
- Florin Dinu <florin.dinu@epfl.ch>
