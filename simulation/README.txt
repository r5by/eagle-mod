The simulator allows you to simulate the behavior of the following systems:
- Hawk 		(published in ATC 15)
- Sparrow 	(published in SOSP 13)
- Eagle 	(as of March 8th 2016 under submission - see EPFL Tech report http://infoscience.epfl.ch/record/216944)
- IdealEagle    (an idealized version of Eagle used as a comparison point for Eagle)
- LWL 		(an LWL implementation used as a comparison point for Eagle)



The simulator takes a number of parameters:
1) path to input trace (e.g. /media/hdd/traces/GOOG.tr)
   This is the input trace that will be simulated
   The trace format is:
   job_submission_time     nr_of_tasks_in_job   average_task_duration   the_runtime_of_each_task
   For example:
   10   3   20   10 20 30

2) stealing enabled/disabled (yes/no)
   Whether to use stealing or not for short tasks.
   This is very important for the performance of Hawk for example.

3) scheduled long jobs in a centralized fashion or not (yes/no)
   If this is set to no, then long jobs are scheduled randomly using a Sparrow-like approach.
   If this is set to yes, then each long task is placed on the worker that currently has the lowest waiting time.

3) cutoff runtime to distinguish long vs short jobs in this experiment (float)
   All jobs that have an average task duration smaller than this cutoff are considered to be short.
   The rest are considered to be long.
   Set this to -1 if you want all jobs to be scheduled as long jobs.

4) cutoff runtime to distinguish long vs short jobs in general (float)
   See later for discussion of the importance of these two cutoff and why there are two of them

5) small partition (float, >=0 and <=100)
   Percent of the total number of worker workers that can be used to schedule short tasks.
   These workers are selected starting from the one with ID 0.
   For example, if you simulate 100 workers (IDs 0 to 99) and you set this parameter to 20
   then only workers with IDs 0 to 19 will be used for scheduling short tasks.

6) big partition (float, >=0, <=100)
   Percent of the total number of worker workers that can be used to schedule long tasks.
   These workers are selected starting from the one with the maximum ID.
   For example, if you simulate 100 workers (IDs 0 to 99) and you set this parameter to 20
   then only IDs 80 to 99 will be used for scheduling long tasks.

7) slots per worker (1)
   This should be set to 1 as the current version of the simulator does not support multiple slots per worker.
   Nevertheless, this is equivalent to having workers with multiple slots where each slots has its own task queue
   and each slot is treated independently from the other slots on the same worker worker.

8) probe ratio (integer)
   The ratio of probes to tasks used in Sparrow-style scheduling.
   For example, if a job has 10 tasks and this parameter is set to 2, then 2*10=20 probes are sent (see Sparrow paper).
   Most experiments in the Sparrow and Hawk papers used a value of 2 for this parameter.

9) monitor interval (float)
   The time interval at which a snapshot of the cluster utilization is printed.
   Setting this to a value of 5 worked well for the traces used in the Hawk paper (ATC 15).
 
10) how to compute an estimate for task runtimes in a job 
   Hawk and Eagle use the average task duration (from the trace) in a job as an estimate for the running time of all tasks in that job.
   This parameter can be used to tweak how that estimate is obtained.
   A value of MEAN means that the average task duration is used as an estimate
   A value of CONSTANT means that the estimate is the average task duration multiplied by a number (see the off_mean_top parameter):  self.estimated_task_duration = mean_task_duration + off_mean_top*mean_task_duration
   A value of RANDOM means that the estimate is chosen randomly from an interval around the average task duration: self.estimated_task_duration = random.uniform(off_mean_bottom*mean_task_duration, off_mean_top*mean_task_duration)

11) off_mean_bottom (> 0, see parameter 10 also)
    May be used in conjunction with parameter 10

12) off_mean_top (>= off_mean_bottom, see parameter 10 also)
    May be used in conjunction with parameter 10

13) stealing strategy (possible values are ATC and RANDOM)
    If stealing is used (parameter nr 2 is set to yes) then this parameter selects the stealing strategy.
    ATC is the strategy used by Hawk in the ATC 15 paper.
    RANDOM steals short task randomly from a worker's queue.

14) Limit to the nr of probes that can be stolen from a worker (integer >0)

15) Limit to the number of workers that can be contacted for stealing
    If nothing can be stolen from any of these workers then the idle worker gives up for now.
    It will retry to steal once it becomes idle again in the future

16) total number of slots simulated (integer > 0)

17) system simulated (possible values, Hawk, Eagle, IdealEagle, LWL)	
    The letter case matters.

=====================================================================================================================================
Example parameter settings:

#This simulates Hawk on the Yahoo trace, with 4000 slots (0 to 3999).
#Short tasks are scheduled over slots 0 to 4000 while long task are only scheduled over slots 800 to 3999.
/media/hdd/HawkEagle/traces2/YH.tr yes yes 90.5811 90.5811 100 98 1 2 5 MEAN 0 0 ATC 10000 10 4000 Hawk

#This simulates Sparrow on the Yahoo trace
#yes, the simulated system is still Hawk but notice the change in parameters that make it behave like Sparrow
/media/hdd/HawkEagle/traces2/YH.tr no no 90.5811 90.5811 100 100 1 2 5 MEAN 0 0 ATC 10000 10 4000 Hawk

#This simulates Eagle on the Yahoo trace
/media/hdd/HawkEagle/traces2/YH.tr no yes 90.5811 90.5811 100 98 1 2 5 MEAN 0 0 ATC 10000 10 4000 Eagle

#This simulates IdealEagle on the Yahoo trace
/media/hdd/HawkEagle/traces2/YH.tr no yes 90.5811 90.5811 100 98 1 2 5 MEAN 0 0 ATC 10000 10 4000 IdealEagle

#This simulates LWL (with partitioning) on the Yahoo trace
/media/hdd/HawkEagle/traces2/YH.tr no yes -1 90.5811 100 98 1 2 5 MEAN 0 0 ATC 10000 10 4000 LWL


Partitioning and cutoff parameters for the various traces:
GOOG 	cutoff: 1129.532	small partition, big partition: 100, 83
CCc 	cutoff: 272.7830	small partition, big partition: 100, 91
YH 	cutoff: 90.5811		small partition, big partition: 100, 98
FB	cutoff: 76.5951		small partition, big partition: 100, 98

=====================================================================================================================================
Why are there two cutoff parameter values?

The main reason is to have a fair comparison between different systems.

In the current implementation, the value of parameter #3 dictates which jobs are considered short vs long for a specific run.
Let SS to be the set of short jobs and SL the set of resulting long jobs.
The two sets may be scheduled in a different fashion (in Hawk, SS would be scheduled distributedly and SL centralized)

Now, for comparison purposes, assume that you want to try a different system that schedules all jobs in a centralized fashion.
This would be achieved by setting parameter nr 3 to -1. 
But this would result in an empty SS set (let's call it SS1) and a larger SL set (let's call it SL1).

Comparing SL1 to SL is not fair because the two sets are not composed of the same jobs.

This is where parameter #4 comes in. This should all times be set to the cutoff for the trace and thus will provide a consistent
separation in short and long jobs regardless of how each set is scheduled.

Put it another way
- parameter #3 controls scheduling
               it dictates which jobs are short and long for scheduling purposes for a specific run.
- parameter #4 consistently divides the jobs into two categories
               it dictates which jobs are short and long for comparison purposes between different systems and configurations.

=====================================================================================================================================
How do you get the numbers for the job running times?

Each simulation outputs a file called "finished_file" which contains informaton about the jobs (one line for each job).
Short jobs (according to parameter #4) are labelled with "by_def:  0" while long jobs are labelled with "by_def:  1".
To present results for Hawk and Eagle, for each type of job (short/long) we collected the running times and then compute a CDF over time.

=====================================================================================================================================
Other consideration:

- We strongly recommend using pypy instead of python to run the simulations.
  With pypy simulations can finish 5-6 times faster.
 
- In extreme cases where a lot of workers are simulated the memory utilization can reach a few GB.
  For the Google trace running with 15000 workers the memory utilization was 2-3GB.
  The maximum we have seen was about 10GB for the Facebook trace with 170000 workers.

=====================================================================================================================================

