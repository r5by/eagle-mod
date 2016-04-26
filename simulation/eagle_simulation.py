#
# EAGLE 
#
# Copyright 2016 Operating Systems Laboratory EPFL
#
# Modified from Sparrow - University of California, Berkeley 
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#



import sys
import time
import logging
import math
import random
import Queue
import bitmap
import copy
import collections

class TaskDurationDistributions:
    CONSTANT, MEAN, FROM_FILE  = range(3)
class EstimationErrorDistribution:
    CONSTANT, RANDOM, MEAN  = range(3)

class Job(object):
    job_count = 1

    def __init__(self, task_distribution, line, estimate_distribution, off_mean_bottom, off_mean_top):
        global job_start_tstamps

        job_args                    = (line.split('\n'))[0].split()
        self.start_time             = float(job_args[0])
        self.num_tasks              = int(job_args[1])
        mean_task_duration          = int(float(job_args[2]))

        #dephase the incoming job in case it has the exact submission time as another already submitted job
        if self.start_time not in job_start_tstamps:
            job_start_tstamps[self.start_time]=self.start_time
        else:
            job_start_tstamps[self.start_time]+=0.01
            self.start_time=job_start_tstamps[self.start_time]
        
        self.id = Job.job_count
        Job.job_count += 1
        self.completed_tasks_count = 0
        self.end_time = self.start_time
        self.unscheduled_tasks = collections.deque()
        self.off_mean_bottom = off_mean_bottom
        self.off_mean_top = off_mean_top

        self.job_type_for_scheduling = BIG if mean_task_duration > CUTOFF_THIS_EXP  else SMALL
        self.job_type_for_comparison = BIG if mean_task_duration > CUTOFF_BIG_SMALL else SMALL
        

        if   task_distribution == TaskDurationDistributions.FROM_FILE: 
            self.file_task_execution_time(job_args)
        elif task_distribution == TaskDurationDistributions.CONSTANT:
            while len(self.unscheduled_tasks) < self.num_tasks:
                self.unscheduled_tasks.appendleft(mean_task_duration)

        self.estimate_distribution = estimate_distribution

        if   estimate_distribution == EstimationErrorDistribution.MEAN:
            self.estimated_task_duration = mean_task_duration
        elif estimate_distribution == EstimationErrorDistribution.CONSTANT:
            self.estimated_task_duration = mean_task_duration+off_mean_top*mean_task_duration
        elif estimate_distribution == EstimationErrorDistribution.RANDOM:
            top = off_mean_top*mean_task_duration
            bottom = off_mean_bottom*mean_task_duration
            self.estimated_task_duration = int(random.uniform(bottom,top)) 
            assert(self.estimated_task_duration<=int(top))
            assert(self.estimated_task_duration>=int(bottom))

        self.probed_workers = set()

    #Job class    """ Returns true if the job has completed, and false otherwise. """
    def update_task_completion_details(self, completion_time):
        self.completed_tasks_count += 1
        self.end_time = max(completion_time, self.end_time)
        assert self.completed_tasks_count <= self.num_tasks
        return self.num_tasks == self.completed_tasks_count


    #Job class
    def file_task_execution_time(self, job_args):
        for task_duration in (job_args[3:]):
           self.unscheduled_tasks.appendleft(int(float(task_duration)))
        assert(len(self.unscheduled_tasks) == self.num_tasks)


#####################################################################################################################
#####################################################################################################################
class Stats(object):

    STATS_STEALING_MESSAGES=0
    STATS_SH_QUEUED_BEHIND_BIG=0
    STATS_SH_EXEC_IN_BP=0
    STATS_TOTAL_STOLEN_TASKS=0
    STATS_SUCCESSFUL_STEAL_ATTEMPTS=0
    STATS_SH_WAITED_FOR_BIG=0
    
#####################################################################################################################
#####################################################################################################################
class Event(object):
    def __init__(self):
        raise NotImplementedError("Event is an abstract class and cannot be instantiated directly")

    def run(self, current_time):
        """ Returns any events that should be added to the queue. """
        raise NotImplementedError("The run() method must be implemented by each class subclassing Event")

#####################################################################################################################
#####################################################################################################################

class JobArrival(Event, file):

    def __init__(self, simulation, task_distribution, job, jobs_file):
        self.simulation = simulation
        self.task_distribution = task_distribution
        self.job = job
        self.jobs_file = jobs_file

    def run(self, current_time):
        new_events = []

        long_job = self.job.estimated_task_duration > CUTOFF_THIS_EXP
        worker_indices = []

        btmap = None
        if not long_job:
            print current_time, ": Short Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration

            if SYSTEM_SIMULATED == "Hawk" or SYSTEM_SIMULATED == "Eagle":
                possible_worker_indices = self.simulation.small_partition_workers
            if SYSTEM_SIMULATED == "IdealEagle":
                possible_worker_indices = self.simulation.get_list_non_long_job_workers_from_btmap(self.simulation.cluster_status_keeper.get_btmap())
            
            worker_indices = self.simulation.find_workers_random(PROBE_RATIO, self.job.num_tasks, possible_worker_indices)

        else:
            print current_time, ":   Big Job arrived!!", self.job.id, " num tasks ", self.job.num_tasks, " estimated_duration ", self.job.estimated_task_duration
            btmap = self.simulation.cluster_status_keeper.get_btmap()

            if (self.simulation.SCHEDULE_BIG_CENTRALIZED):
                workers_queue_status = self.simulation.cluster_status_keeper.get_queue_status()
                if SYSTEM_SIMULATED == "LWL" and self.job.job_type_for_comparison == SMALL:
                    possible_workers = self.simulation.small_partition_workers_hash
                else:
                    possible_workers = self.simulation.big_partition_workers_hash

                worker_indices = self.simulation.find_workers_long_job_prio(self.job.num_tasks, self.job.estimated_task_duration, workers_queue_status, current_time, self.simulation, possible_workers)
                self.simulation.cluster_status_keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration)
            else:
                worker_indices = self.simulation.find_workers_random(PROBE_RATIO, self.job.num_tasks, self.simulation.big_partition_workers)

        new_events = self.simulation.send_probes(self.job, current_time, worker_indices, btmap)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if (line == ''):    return new_events

        self.job = Job(self.task_distribution, line, self.job.estimate_distribution, self.job.off_mean_bottom, self.job.off_mean_top)
        new_events.append((self.job.start_time, self))
        return new_events


#####################################################################################################################
#####################################################################################################################

class PeriodicTimerEvent(Event):
    def __init__(self,simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events=[]

        total_load       =str(int(10000*(1-self.simulation.total_free_slots*1.0/(TOTAL_WORKERS*SLOTS_PER_WORKER)))/100.0)
        small_load       =str(int(10000*(1-self.simulation.free_slots_small_partition*1.0/len(self.simulation.small_partition_workers)))/100.0)
        big_load         =str(int(10000*(1-self.simulation.free_slots_big_partition*1.0/len(self.simulation.big_partition_workers)))/100.0)
        small_not_big_load ="N/A"
        if(len(self.simulation.small_not_big_partition_workers)!=0):
                small_not_big_load        =str(int(10000*(1-self.simulation.free_slots_small_not_big_partition*1.0/len(self.simulation.small_not_big_partition_workers)))/100.0)

        print >> load_file,"total_load: "+ total_load + " small_load: "+small_load+ " big_load: "+big_load+ " small_not_big_load: "+small_not_big_load+" current_time: " + str(current_time) 


        if(not self.simulation.event_queue.empty()):
            new_events.append((current_time+MONITOR_INTERVAL,self))
        return new_events


#####################################################################################################################
#####################################################################################################################

class ProbeEvent(Event):
    def __init__(self, worker, job_id, task_length, btmap):
        self.worker = worker
        self.job_id = job_id
        self.task_length = task_length
        self.btmap=btmap

    def run(self, current_time):
        return self.worker.add_probe(self.job_id, self.task_length, current_time,self.btmap)

#####################################################################################################################
#####################################################################################################################

class ClusterStatusKeeper():
    def __init__(self):
        self.worker_queues = {}
        self.btmap = bitmap.BitMap(TOTAL_WORKERS)
        for i in range(0, TOTAL_WORKERS):
           self.worker_queues[i] = 0


    def get_queue_status(self):
        return self.worker_queues

    def get_btmap(self):
        return self.btmap

    def update_workers_queue(self, worker_indices, increase, duration):
        for worker in worker_indices:
           queue = self.worker_queues[worker]
           if increase:
                queue += duration
                self.btmap.set(worker) 
           else:
                queue -= duration
                if(queue == 0):
                    self.btmap.flip(worker) 
           assert queue >= 0
           self.worker_queues[worker] = queue

#####################################################################################################################
#####################################################################################################################

class NoopGetTaskResponseEvent(Event):
    def __init__(self, worker):
        self.worker = worker

    def run(self, current_time):
        return self.worker.free_slot(current_time)

#####################################################################################################################
#####################################################################################################################

class TaskEndEvent():
    def __init__(self, worker, SCHEDULE_BIG_CENTRALIZED, status_keeper, job_id, estimated_task_duration,real_task_duration, job_completed, simulation):
        self.worker = worker
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.status_keeper = status_keeper
        self.estimated_minus_task_duration = estimated_task_duration-real_task_duration
        self.estimated_task_duration = estimated_task_duration
        self.job_id = job_id
        self.job_completed = job_completed
        self.simulation = simulation

    def run(self, current_time):
        global stats
        if(not (self.estimated_task_duration > CUTOFF_THIS_EXP)):
            if(self.worker.in_big):
                stats.STATS_SH_EXEC_IN_BP += 1                

        elif (self.SCHEDULE_BIG_CENTRALIZED):
            self.status_keeper.update_workers_queue([self.worker.id], False, self.estimated_task_duration)

        self.worker.tstamp_start_crt_big_task=-1
        return self.worker.free_slot(current_time)


#####################################################################################################################
#####################################################################################################################

class Worker(object):
    def __init__(self, simulation, num_slots, id, index_last_small, index_first_big):
        
        self.simulation = simulation

        # List of times when slots were freed, for each free slot (used to track the time the worker spends idle).
        self.free_slots = []
        while len(self.free_slots) < num_slots:
            self.free_slots.append(0)

        self.queued_big = 0
        self.queued_probes = []
        self.id = id
        self.executing_big = False

        self.tstamp_start_crt_big_task=-1
        self.estruntime_crt_task=-1

        self.in_small           =False
        self.in_big             =False
        self.in_small_not_big   =False

        if(id<=index_last_small):       self.in_small=True
        if(id>=index_first_big):        self.in_big=True
        if(id<index_first_big):         self.in_small_not_big=True

        self.btmap=None
        self.btmap_tstamp=-1


    #Worker class
    def add_probe(self, job_id, task_length, current_time, btmap):
        global stats

        if (task_length <= CUTOFF_THIS_EXP and (self.executing_big==True or self.queued_big>0)):
            stats.STATS_SH_QUEUED_BEHIND_BIG+=1        

        self.queued_probes.append((job_id,task_length,(self.executing_big==True or self.queued_big>0)))

        if (task_length > CUTOFF_THIS_EXP):
            self.queued_big     = self.queued_big + 1
            self.btmap          = copy.deepcopy(btmap)
            self.btmap_tstamp   = current_time

        if len(self.queued_probes) > 0 and len(self.free_slots) > 0:
            return self.process_next_probe_in_the_queue(current_time)
        else:
            return []



    #Worker class
    def free_slot(self, current_time):
        self.free_slots.append(current_time)
        self.simulation.increase_free_slots_for_load_tracking(self)
        self.executing_big=False

        if len(self.queued_probes) > 0:
            return self.process_next_probe_in_the_queue(current_time)
        
        if len(self.queued_probes) == 0 and self.simulation.stealing_allowed == True:
            return self.ask_probes(current_time)
        
        return []



    #Worker class
    def ask_probes(self, current_time):
        global stats

        new_events  =  []
        new_probes  =  []
        ctr_it      =  0
        from_worker = -1

        while(ctr_it < STEALING_ATTEMPTS and len(new_probes)==0):
            from_worker,new_probes = self.simulation.get_probes(current_time, self.id)
            ctr_it += 1

        if(from_worker!=-1 and len(new_probes)!=0):
            print current_time, ": Worker ", self.id," Stealing: ", len(new_probes), " from: ",from_worker, " attempts: ",ctr_it
            stats.STATS_STEALING_MESSAGES           += ctr_it
            stats.STATS_TOTAL_STOLEN_TASKS          += len(new_probes)
            stats.STATS_SUCCESSFUL_STEAL_ATTEMPTS   += 1
        else:
            print current_time, ": Worker ", self.id," failed to steal. attempts: ",ctr_it
            stats.STATS_STEALING_MESSAGES += ctr_it

        for job_id, task_length, behind_big in new_probes:
            assert (task_length <= CUTOFF_THIS_EXP)
            new_events.extend(self.add_probe(job_id, task_length, current_time, None))

        return new_events



    #Worker class
    def get_probes_atc(self, current_time, free_worker_id):
        probes_to_give = []

        i = 0
        skipped_small=0
        skipped_big=0

        if not self.executing_big:
            while (i < len(self.queued_probes) and self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                i += 1
        skipped_short=i

        while (i < len(self.queued_probes) and self.queued_probes[i][1] > CUTOFF_THIS_EXP):
            i += 1
        skipped_big=i-skipped_short

        total_time_probes = 0

        nr_short_chosen=0
        if (i < len(self.queued_probes)):
            while (len(self.queued_probes) > i and self.queued_probes[i][1] <= CUTOFF_THIS_EXP and nr_short_chosen < STEALING_LIMIT):
                nr_short_chosen +=1
                probes_to_give.append(self.queued_probes.pop(i))
                total_time_probes += probes_to_give[-1][1]

        return probes_to_give



    #Worker class
    def get_probes_random(self, current_time, free_worker_id):
        probes_to_give = []
        all_positions_of_short_tasks=[]
        randomly_chosen_short_task_positions=[]
        i=0

        #record the ids (in queued_probes) of the queued short tasks
        while (i < len(self.queued_probes)):
            if(self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                all_positions_of_short_tasks.append(i)
            i += 1


        #select the probes to steal
        i=0
        while (len(all_positions_of_short_tasks) > 0 and len(randomly_chosen_short_task_positions) < STEALING_LIMIT):
            rnd_index = random.randint(0,len(all_positions_of_short_tasks)-1)
            randomly_chosen_short_task_positions.append(all_positions_of_short_tasks.pop(rnd_index))
        randomly_chosen_short_task_positions.sort()

        #remove the selected probes from the worker queue in decreasing order of IDs
        decreasing_ids=len(randomly_chosen_short_task_positions)-1
        total_time_probes = 0
        while(decreasing_ids>=0):
            probes_to_give.append(self.queued_probes.pop(randomly_chosen_short_task_positions[decreasing_ids]))
            total_time_probes += probes_to_give[-1][1]
            decreasing_ids-=1
        probes_to_give=probes_to_give[::-1]  #reverse the list of tuples

        return probes_to_give



    #Worker class
    def process_next_probe_in_the_queue(self, current_time):
        global stats

        self.free_slots.pop(0)
        self.simulation.decrease_free_slots_for_load_tracking(self)

        job_id = self.queued_probes[0][0]
        estimated_task_duration = self.queued_probes[0][1]

        self.executing_big = estimated_task_duration > CUTOFF_THIS_EXP
        if self.executing_big:
            self.queued_big                 = self.queued_big -1
            self.tstamp_start_crt_big_task  = current_time
            self.estruntime_crt_task        = estimated_task_duration
        else:
            self.tstamp_start_crt_big_task=-1
            if (self.queued_probes[0][2]==True):
               stats.STATS_SH_WAITED_FOR_BIG+=1

        self.queued_probes              =   self.queued_probes[1:]
        return self.simulation.get_task(job_id, self, current_time)


#####################################################################################################################
#####################################################################################################################

class Simulation(object):
    def __init__(self, monitor_interval, stealing_allowed, SCHEDULE_BIG_CENTRALIZED, WORKLOAD_FILE,small_job_th,cutoff_big_small,ESTIMATION,off_mean_bottom,off_mean_top,nr_workers):

        CUTOFF_THIS_EXP = float(small_job_th)
        TOTAL_WORKERS = int(nr_workers)
        self.total_free_slots = SLOTS_PER_WORKER * TOTAL_WORKERS
        self.jobs = {}
        self.event_queue = Queue.PriorityQueue()
        self.workers = []

        self.index_last_worker_of_small_partition = int(SMALL_PARTITION*TOTAL_WORKERS*SLOTS_PER_WORKER/100)-1
        self.index_first_worker_of_big_partition  = int((100-BIG_PARTITION)*TOTAL_WORKERS*SLOTS_PER_WORKER/100)

        while len(self.workers) < TOTAL_WORKERS:
            self.workers.append(Worker(self, SLOTS_PER_WORKER, len(self.workers),self.index_last_worker_of_small_partition,self.index_first_worker_of_big_partition))
        self.worker_indices = range(TOTAL_WORKERS)
        self.off_mean_bottom = off_mean_bottom
        self.off_mean_top = off_mean_top
        self.ESTIMATION = ESTIMATION

        print "self.index_last_worker_of_small_partition:         ", self.index_last_worker_of_small_partition
        print "self.index_first_worker_of_big_partition:          ", self.index_first_worker_of_big_partition

        self.small_partition_workers_hash =  {}
        self.big_partition_workers_hash = {}
        self.small_not_big_partition_workers_hash = {}

        self.small_partition_workers = self.worker_indices[:self.index_last_worker_of_small_partition+1]    # so not including the worker after :
        for node in self.small_partition_workers:
            self.small_partition_workers_hash[node]=1

        self.big_partition_workers = self.worker_indices[self.index_first_worker_of_big_partition:]         # so including the worker before :
        for node in self.big_partition_workers:
            self.big_partition_workers_hash[node]=1

        self.small_not_big_partition_workers=self.worker_indices[:self.index_first_worker_of_big_partition]  # so not including the worker after: 
        for node in self.small_not_big_partition_workers:
            self.small_not_big_partition_workers_hash[node]=1

        print "Size of self.small_partition_workers_hash:         ", len(self.small_partition_workers_hash)
        print "Size of self.big_partition_workers_hash:           ", len(self.big_partition_workers_hash)
        print "Size of self.small_not_big_partition_workers_hash: ", len(self.small_not_big_partition_workers_hash)


        self.free_slots_small_partition = len(self.small_partition_workers)
        self.free_slots_big_partition = len(self.big_partition_workers)
        self.free_slots_small_not_big_partition = len(self.small_not_big_partition_workers)

        self.cluster_status_keeper = ClusterStatusKeeper()
        self.stealing_allowed = stealing_allowed
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.WORKLOAD_FILE = WORKLOAD_FILE
        self.btmap_tstamp = -1
        self.clusterstatus_from_btmap = None


    #Simulation class
    def subtract_sets(self, set1, set2):
        used={}
        difference=[]
        ctr_long=0

        for item in set2:
            used[item]=1

        for worker in set1:
            if not worker in set2:
                difference.append(worker)

        return difference


    #Simulation class
    def find_workers_random(self, probe_ratio, nr_tasks, possible_worker_indices):
        chosen_worker_indices=[]
        for it in range(0,probe_ratio*nr_tasks):
                rnd_index = random.randint(0,len(possible_worker_indices)-1)
                chosen_worker_indices.append(possible_worker_indices[rnd_index])
        return chosen_worker_indices


    #Simulation class
    def find_workers_long_job_prio(self, num_tasks, estimated_task_duration, workers_queue_status,current_time,simulation, hash_workers_considered):

        chosen_worker_indices= []
        workers_needed = num_tasks
        prio_queue = Queue.PriorityQueue()

        empty_nodes=[]  #performance optimization
        for index in hash_workers_considered:
            qlen          = workers_queue_status[index]                
            worker_obj    = simulation.workers[index]
            worker_id     = index

            start_of_crt_big_task = worker_obj.tstamp_start_crt_big_task
            assert(current_time >= start_of_crt_big_task)
            adjusted_waiting_time=qlen

            if qlen == 0 :
                empty_nodes.append(worker_id)
                if(len(empty_nodes)==workers_needed):
                    break
            else: 
                if(start_of_crt_big_task != -1):
                    executed_so_far=current_time-start_of_crt_big_task
                    estimated_crt_task=worker_obj.estruntime_crt_task
                    adjusted_waiting_time=2*NETWORK_DELAY+qlen-min(executed_so_far,estimated_crt_task)

                assert adjusted_waiting_time >=0                 
                prio_queue.put((adjusted_waiting_time,worker_id))

        #performance optimization 
        if(len(empty_nodes) == workers_needed):
            return empty_nodes
        else:
            chosen_worker_indices=empty_nodes
            for nodeid in chosen_worker_indices:
                prio_queue.put((estimated_task_duration,nodeid))            

        
        queue_length,worker= prio_queue.get()
        while (workers_needed > len(chosen_worker_indices)):

            next_queue_length,next_worker=prio_queue.get()
            while(queue_length<=next_queue_length and len(chosen_worker_indices) < workers_needed):
                    chosen_worker_indices.append(worker)
                    queue_length += estimated_task_duration

            prio_queue.put((queue_length,worker));
            queue_length=next_queue_length
            worker = next_worker 

        assert workers_needed == len(chosen_worker_indices)
        print chosen_worker_indices
        return chosen_worker_indices


    #Simulation class
    def send_probes(self, job, current_time, worker_indices, btmap):
        if SYSTEM_SIMULATED == "Hawk" or SYSTEM_SIMULATED == "IdealEagle":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "Eagle":
            return self.send_probes_eagle(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "LWL":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)


    #Simulation class
    def send_probes_hawk(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job

        probe_events = []
        for worker_index in worker_indices:
            probe_events.append((current_time + NETWORK_DELAY, ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration,btmap)))
            job.probed_workers.add(worker_index)
        return probe_events


    #Simulation class
    def try_round_of_probing(self, current_time, job, worker_list, probe_events, roundnr):
            successful_worker_indices = []
            id_worker_with_newest_btmap = -1
            freshest_btmap_tstamp = self.btmap_tstamp

            for worker_index in worker_list:
                if(not self.workers[worker_index].executing_big and not self.workers[worker_index].queued_big):
                    probe_events.append((current_time + NETWORK_DELAY*(roundnr/2+1), ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration, None)))
                    job.probed_workers.add(worker_index)
                    successful_worker_indices.append(worker_index)
                if (self.workers[worker_index].btmap_tstamp > freshest_btmap_tstamp):
                    id_worker_with_newest_btmap = worker_index
                    freshest_btmap_tstamp       = self.workers[worker_index].btmap_tstamp

            if (id_worker_with_newest_btmap != -1):                    
                    self.btmap = copy.deepcopy(self.workers[id_worker_with_newest_btmap].btmap)
                    self.btmap_tstamp = self.workers[id_worker_with_newest_btmap].btmap_tstamp

            missing_probes = len(worker_list)-len(successful_worker_indices)
            return len(successful_worker_indices), successful_worker_indices
        

    #Simulation class
    def get_list_non_long_job_workers_from_btmap(self,btmap):
        non_long_job_workers=[]
            
        non_long_job_workers=self.small_not_big_partition_workers[:]

        for index in self.big_partition_workers:
            if not btmap.test(index):
                non_long_job_workers.append(index)
        return non_long_job_workers

    #Simulation class
    def send_probes_eagle(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job
        probe_events = []

        if (job.estimated_task_duration > CUTOFF_THIS_EXP):
            for worker_index in worker_indices:
                probe_events.append((current_time + NETWORK_DELAY, ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration, btmap)))
                job.probed_workers.add(worker_index)

        else:
            missing_probes=len(worker_indices)
            self.btmap_tstamp = -1
            self.btmap = None
            ROUNDS_SCHEDULING=5       
 
            for i in range(0,ROUNDS_SCHEDULING):
                ok_nr, ok_nodes = self.try_round_of_probing(current_time,job,worker_indices,probe_events,i+1)  
                missing_probes -= ok_nr
                if(missing_probes==0):
                    return probe_events
                
                list_non_long_job_workers = self.get_list_non_long_job_workers_from_btmap(self.btmap)
                worker_indices = self.find_workers_random(1, missing_probes, list_non_long_job_workers)

            if(missing_probes>0):
                worker_indices = self.find_workers_random(1, missing_probes, self.small_not_big_partition_workers)
                self.try_round_of_probing(current_time,job,worker_indices,probe_events,ROUNDS_SCHEDULING+1)  
        
        return probe_events



    #Simulation class
    #bookkeeping for tracking the load
    def increase_free_slots_for_load_tracking(self, worker):
        self.total_free_slots += 1
        if(worker.in_small):                self.free_slots_small_partition+=1
        if(worker.in_big):                  self.free_slots_big_partition+=1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition+=1


    #Simulation class
    #bookkeeping for tracking the load
    def decrease_free_slots_for_load_tracking(self, worker):
        self.total_free_slots -= 1
        if(worker.in_small):                self.free_slots_small_partition-=1
        if(worker.in_big):                  self.free_slots_big_partition-=1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition-=1


    #Simulation class
    def get_task(self, job_id, worker, current_time):
        job = self.jobs[job_id]
        #account for the fact that this is called when the probe is launched but it needs an RTT to talk to the scheduler
        get_task_response_time = current_time + 2*NETWORK_DELAY
        
        if len(job.unscheduled_tasks) == 0 :
            return [(get_task_response_time, NoopGetTaskResponseEvent(worker))]

        events = []
        task_duration = job.unscheduled_tasks.pop()
        task_completion_time = task_duration + get_task_response_time
        print current_time, " worker:",worker.id," task from job ",job_id," task duration: ",task_duration, " will finish at time ", task_completion_time
        is_job_complete = job.update_task_completion_details(task_completion_time)

        if is_job_complete:
            print >> finished_file, task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " by_def: ",job.job_type_for_comparison, " total_job_running_time: ",(job.end_time - job.start_time)

        events.append((task_completion_time, TaskEndEvent(worker, self.SCHEDULE_BIG_CENTRALIZED, self.cluster_status_keeper, job.id, job.estimated_task_duration, task_duration, (len(job.unscheduled_tasks) == 0), self)))

        if len(job.unscheduled_tasks) == 0:
            logging.info("Finished scheduling tasks for job %s" % job.id)
        return events


    #Simulation class
    def get_probes(self, current_time, free_worker_id):
        worker_index = random.randint(self.index_first_worker_of_big_partition, len(self.workers)-1)
        if     (STEALING_STRATEGY ==  "ATC"):       probes = self.workers[worker_index].get_probes_atc(current_time, free_worker_id)
        elif (STEALING_STRATEGY == "RANDOM"):       probes = self.workers[worker_index].get_probes_random(current_time, free_worker_id)

        return worker_index,probes


    #Simulation class
    def run(self):
        last_time = 0

        self.jobs_file = open(self.WORKLOAD_FILE, 'r')

        self.task_distribution = TaskDurationDistributions.FROM_FILE

        estimate_distribution = EstimationErrorDistribution.MEAN
        if(self.ESTIMATION == "MEAN"):
            estimate_distribution = EstimationErrorDistribution.MEAN
            self.off_mean_bottom = self.off_mean_top = 0
        elif(self.ESTIMATION == "CONSTANT"):
            estimate_distribution = EstimationErrorDistribution.CONSTANT
            assert(self.off_mean_bottom == self.off_mean_top)
        elif(self.ESTIMATION == "RANDOM"):
            estimate_distribution = EstimationErrorDistribution.RANDOM
            assert(self.off_mean_bottom > 0)
            assert(self.off_mean_top > 0)
            assert(self.off_mean_top>self.off_mean_bottom)

        line = self.jobs_file.readline()
        new_job = Job(self.task_distribution, line, estimate_distribution, self.off_mean_bottom, self.off_mean_top)
        self.event_queue.put((float(line.split()[0]), JobArrival(self, self.task_distribution, new_job, self.jobs_file)))
        self.event_queue.put((float(line.split()[0]), PeriodicTimerEvent(self)))

        while (not self.event_queue.empty()):
            current_time, event = self.event_queue.get()
            assert current_time >= last_time
            last_time = current_time
            new_events = event.run(current_time)
            for new_event in new_events:
                self.event_queue.put(new_event)

        print "Simulation ending, no more events"
        self.jobs_file.close()

#####################################################################################################################
#globals

finished_file   = open('finished_file', 'w')
load_file       = open('load_file', 'w')
stats_file      = open('stats_file', 'w')

NETWORK_DELAY = 0.0005
BIG=1
SMALL=0

job_start_tstamps={}

if(len(sys.argv)!=19):
    print "Incorrent number of parameters."
    sys.exit(1)


WORKLOAD_FILE                   = sys.argv[1]
stealing                        = (sys.argv[2] == "yes")
SCHEDULE_BIG_CENTRALIZED        = (sys.argv[3] == "yes")
CUTOFF_THIS_EXP                 = float(sys.argv[4])        #
CUTOFF_BIG_SMALL                = float(sys.argv[5])        #
SMALL_PARTITION                 = float(sys.argv[6])          #from the start of worker_indices
BIG_PARTITION                   = float(sys.argv[7])          #from the end of worker_indices
SLOTS_PER_WORKER                = int(sys.argv[8])
PROBE_RATIO                     = int(sys.argv[9])
MONITOR_INTERVAL                = int(sys.argv[10])
ESTIMATION                      = sys.argv[11]              #MEAN, CONSTANT or RANDOM
OFF_MEAN_BOTTOM                 = float(sys.argv[12])       # > 0
OFF_MEAN_TOP                    = float(sys.argv[13])       # >= OFF_MEAN_BOTTOM
STEALING_STRATEGY               = sys.argv[14]
STEALING_LIMIT                  = int(sys.argv[15])         #cap on the nr of tasks to steal from one node
STEALING_ATTEMPTS               = int(sys.argv[16])         #cap on the nr of nodes to contact for stealing
TOTAL_WORKERS                   = int(sys.argv[17])
SYSTEM_SIMULATED                = sys.argv[18]  

t1=time.time()

stats = Stats()
s = Simulation(MONITOR_INTERVAL, stealing, SCHEDULE_BIG_CENTRALIZED, WORKLOAD_FILE,CUTOFF_THIS_EXP,CUTOFF_BIG_SMALL,ESTIMATION,OFF_MEAN_BOTTOM,OFF_MEAN_TOP,TOTAL_WORKERS)
s.run()

print "Simulation ended in ", (time.time()-t1), " s "

finished_file.close()
load_file.close()

print >> stats_file, "STATS_SH_QUEUED_BEHIND_BIG:      ",        stats.STATS_SH_QUEUED_BEHIND_BIG 
print >> stats_file, "STATS_SH_EXEC_IN_BP:             ",               stats.STATS_SH_EXEC_IN_BP
print >> stats_file, "STATS_SH_WAITED_FOR_BIG:         ",           stats.STATS_SH_WAITED_FOR_BIG
print >> stats_file, "                                 "
print >> stats_file, "STATS_TOTAL_STOLEN_TASKS:        ",          stats.STATS_TOTAL_STOLEN_TASKS 
print >> stats_file, "STATS_SUCCESSFUL_STEAL_ATTEMPTS: ",   stats.STATS_SUCCESSFUL_STEAL_ATTEMPTS
print >> stats_file, "STATS_STEALING_MESSAGES:         ",           stats.STATS_STEALING_MESSAGES
stats_file.close()
