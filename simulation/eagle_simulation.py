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
    per_job_task_info = {}

    def __init__(self, task_distribution, line, estimate_distribution, off_mean_bottom, off_mean_top):
        global job_start_tstamps

        job_args                    = (line.split('\n'))[0].split()
        self.start_time             = float(job_args[0])
        self.num_tasks              = int(job_args[1])
        mean_task_duration          = int(float(job_args[2]))

        #dephase the incoming job in case it has the exact submission time as another already submitted job
        if self.start_time not in job_start_tstamps:
            job_start_tstamps[self.start_time] = self.start_time
        else:
            job_start_tstamps[self.start_time] += 0.01
            self.start_time = job_start_tstamps[self.start_time]
        
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
            self.estimated_task_duration = int(mean_task_duration+off_mean_top*mean_task_duration)
        elif estimate_distribution == EstimationErrorDistribution.RANDOM:
            top = off_mean_top*mean_task_duration
            bottom = off_mean_bottom*mean_task_duration
            self.estimated_task_duration = int(random.uniform(bottom,top)) 
            assert(self.estimated_task_duration <= int(top))
            assert(self.estimated_task_duration >= int(bottom))

        self.probed_workers = set()
        self.remaining_exec_time = self.estimated_task_duration*len(self.unscheduled_tasks)

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

    #Job class
    def update_remaining_time(self):
        self.remaining_exec_time -= self.estimated_task_duration
        assert(self.remaining_exec_time >=0)
        if (len(self.unscheduled_tasks) == 0): #Improvement
            remaining_exec_time = -1

#####################################################################################################################
#####################################################################################################################
class Stats(object):

    STATS_STEALING_MESSAGES = 0
    STATS_SH_QUEUED_BEHIND_BIG = 0
    STATS_SH_EXEC_IN_BP = 0
    STATS_TOTAL_STOLEN_TASKS = 0
    STATS_SUCCESSFUL_STEAL_ATTEMPTS = 0
    STATS_SH_WAITED_FOR_BIG = 0
    STATS_STICKY_PROBES = 0
    
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

        long_job = self.job.job_type_for_scheduling == BIG
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

            if (SYSTEM_SIMULATED == "DLWL"):
                if self.job.job_type_for_comparison == SMALL:
                    possible_workers = self.simulation.small_partition_workers_hash
                else:
                    possible_workers = self.simulation.big_partition_workers_hash
                estimated_task_durations = [self.job.estimated_task_duration for i in range(len(self.job.unscheduled_tasks))]
                worker_indices = self.simulation.find_workers_dlwl(estimated_task_durations, self.simulation.shared_cluster_status, current_time, self.simulation, possible_workers)
                self.simulation.cluster_status_keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration)
            elif (self.simulation.SCHEDULE_BIG_CENTRALIZED):
                workers_queue_status = self.simulation.cluster_status_keeper.get_queue_status()
                if SYSTEM_SIMULATED == "LWL" and self.job.job_type_for_comparison == SMALL:
                    possible_workers = self.simulation.small_partition_workers_hash
                else:
                    possible_workers = self.simulation.big_partition_workers_hash

                worker_indices = self.simulation.find_workers_long_job_prio(self.job.num_tasks, self.job.estimated_task_duration, workers_queue_status, current_time, self.simulation, possible_workers)
                self.simulation.cluster_status_keeper.update_workers_queue(worker_indices, True, self.job.estimated_task_duration)
            else:
                worker_indices = self.simulation.find_workers_random(PROBE_RATIO, self.job.num_tasks, self.simulation.big_partition_workers)

        Job.per_job_task_info[self.job.id] = {}
        for tasknr in range(0,self.job.num_tasks):
            Job.per_job_task_info[self.job.id][tasknr] =- 1 

        new_events = self.simulation.send_probes(self.job, current_time, worker_indices, btmap)

        # Creating a new Job Arrival event for the next job in the trace
        line = self.jobs_file.readline()
        if (line == ''):
            self.simulation.scheduled_last_job = True
            return new_events

        self.job = Job(self.task_distribution, line, self.job.estimate_distribution, self.job.off_mean_bottom, self.job.off_mean_top)
        new_events.append((self.job.start_time, self))
        self.simulation.jobs_scheduled += 1
        return new_events


#####################################################################################################################
#####################################################################################################################

class PeriodicTimerEvent(Event):
    def __init__(self,simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events = []

        total_load       = str(int(10000*(1-self.simulation.total_free_slots*1.0/(TOTAL_WORKERS*SLOTS_PER_WORKER)))/100.0)
        small_load       = str(int(10000*(1-self.simulation.free_slots_small_partition*1.0/len(self.simulation.small_partition_workers)))/100.0)
        big_load         = str(int(10000*(1-self.simulation.free_slots_big_partition*1.0/len(self.simulation.big_partition_workers)))/100.0)
        small_not_big_load ="N/A"
        if(len(self.simulation.small_not_big_partition_workers)!=0):
            small_not_big_load        = str(int(10000*(1-self.simulation.free_slots_small_not_big_partition*1.0/len(self.simulation.small_not_big_partition_workers)))/100.0)

        print >> load_file,"total_load: " + total_load + " small_load: " + small_load + " big_load: " + big_load + " small_not_big_load: " + small_not_big_load+" current_time: " + str(current_time) 

        if(not self.simulation.event_queue.empty()):
            new_events.append((current_time + MONITOR_INTERVAL,self))
        return new_events


#####################################################################################################################
#####################################################################################################################
#####################################################################################################################

class WorkerHeartbeatEvent(Event):
    
    def __init__(self,simulation):
        self.simulation = simulation

    def run(self, current_time):
        new_events = []
        if(HEARTBEAT_DELAY == 0):
            self.simulation.shared_cluster_status = self.simulation.cluster_status_keeper.get_queue_status()
        else:
            self.simulation.shared_cluster_status = copy.deepcopy(self.simulation.cluster_status_keeper.get_queue_status()) 
     
        if(HEARTBEAT_DELAY != 0): 
            if(self.simulation.jobs_completed != self.simulation.jobs_scheduled or self.simulation.scheduled_last_job == False):
                new_events.append((current_time + HEARTBEAT_DELAY,self))

        return new_events


#####################################################################################################################
#####################################################################################################################

class ProbeEvent(Event):
    def __init__(self, worker, job_id, task_length, job_type_for_scheduling, btmap):
        self.worker = worker
        self.job_id = job_id
        self.task_length = task_length
        self.job_type_for_scheduling = job_type_for_scheduling
        self.btmap = btmap

    def run(self, current_time):
        return self.worker.add_probe(self.job_id, self.task_length, self.job_type_for_scheduling, current_time,self.btmap)

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
            assert queue >= 0, (" offending value for queue: %r %i " % (queue,worker))
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

class UpdateRemainingTimeEvent(Event):
    def __init__(self, job):
        self.job = job

    def run(self, current_time):
        assert( len(self.job.unscheduled_tasks)>=0)
        self.job.update_remaining_time()
        return []

#####################################################################################################################
#####################################################################################################################

class TaskEndEvent():
    def __init__(self, worker, SCHEDULE_BIG_CENTRALIZED, status_keeper, job_id, job_type_for_scheduling, estimated_task_duration, this_task_id):
        self.worker = worker
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.status_keeper = status_keeper
        self.job_id = job_id
        self.job_type_for_scheduling = job_type_for_scheduling
        self.estimated_task_duration = estimated_task_duration
        self.this_task_id = this_task_id

    def run(self, current_time):
        global stats
        if(self.job_type_for_scheduling != BIG):
            if(self.worker.in_big):
                stats.STATS_SH_EXEC_IN_BP += 1

        elif (self.SCHEDULE_BIG_CENTRALIZED):
            self.status_keeper.update_workers_queue([self.worker.id], False, self.estimated_task_duration)

        del Job.per_job_task_info[self.job_id][self.this_task_id]
        #if(len(Job.per_job_task_info[self.job_id])==0):
         #   del Job.per_job_task_info[self.job_id]
            

        self.worker.tstamp_start_crt_big_task =- 1
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

        self.tstamp_start_crt_big_task = -1
        self.estruntime_crt_task = -1

        self.in_small           = False
        self.in_big             = False
        self.in_small_not_big   = False

        if (id <= index_last_small):       self.in_small = True
        if (id >= index_first_big):        self.in_big = True
        if (id < index_first_big):         self.in_small_not_big = True

        self.btmap = None
        self.btmap_tstamp = -1


    #Worker class
    def add_probe(self, job_id, task_length, job_type_for_scheduling, current_time, btmap):
        global stats

        long_job = job_type_for_scheduling == BIG
        if (not long_job and (self.executing_big == True or self.queued_big > 0)):
            stats.STATS_SH_QUEUED_BEHIND_BIG += 1        

        self.queued_probes.append([job_id,task_length,(self.executing_big == True or self.queued_big > 0), 0, False])

        if (long_job):
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
        self.executing_big = False

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

        while(ctr_it < STEALING_ATTEMPTS and len(new_probes) == 0):
            from_worker,new_probes = self.simulation.get_probes(current_time, self.id)
            ctr_it += 1

        if(from_worker!=-1 and len(new_probes)!= 0):
            print current_time, ": Worker ", self.id," Stealing: ", len(new_probes), " from: ",from_worker, " attempts: ",ctr_it
            stats.STATS_STEALING_MESSAGES           += ctr_it
            stats.STATS_TOTAL_STOLEN_TASKS          += len(new_probes)
            stats.STATS_SUCCESSFUL_STEAL_ATTEMPTS   += 1
        else:
            print current_time, ": Worker ", self.id," failed to steal. attempts: ",ctr_it
            stats.STATS_STEALING_MESSAGES += ctr_it

        for job_id, task_length, behind_big, cum, sticky in new_probes:
            assert (self.simulation.jobs[job_id].job_type_for_comparison != BIG)
            new_events.extend(self.add_probe(job_id, task_length, SMALL, current_time, None))

        return new_events



    #Worker class
    def get_probes_atc(self, current_time, free_worker_id):
        probes_to_give = []

        i = 0
        skipped_small = 0
        skipped_big = 0

        if not self.executing_big:
            while (i < len(self.queued_probes) and self.simulation.jobs[self.queued_probes[i][0]].job_type_for_comparison != BIG): #self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                i += 1

        skipped_short = i

        while (i < len(self.queued_probes) and self.simulation.jobs[self.queued_probes[i][0]].job_type_for_comparison == BIG): #self.queued_probes[i][1] > CUTOFF_THIS_EXP):
            i += 1

        skipped_big = i - skipped_short

        total_time_probes = 0

        nr_short_chosen = 0
        if (i < len(self.queued_probes)):
            while (len(self.queued_probes) > i and self.simulation.jobs[self.queued_probes[i][0]].job_type_for_scheduling != BIG and nr_short_chosen < STEALING_LIMIT):
                nr_short_chosen += 1
                probes_to_give.append(self.queued_probes.pop(i))
                total_time_probes += probes_to_give[-1][1]

        return probes_to_give



    #Worker class
    def get_probes_random(self, current_time, free_worker_id):
        probes_to_give = []
        all_positions_of_short_tasks = []
        randomly_chosen_short_task_positions = []
        i = 0

        #record the ids (in queued_probes) of the queued short tasks
        while (i < len(self.queued_probes)):
            big_job = self.simulation.jobs[self.queued_probes[i][0]].job_type_for_scheduling == BIG
            if (not big_job): #self.queued_probes[i][1] <= CUTOFF_THIS_EXP):
                all_positions_of_short_tasks.append(i)
            i += 1


        #select the probes to steal
        i = 0
        while (len(all_positions_of_short_tasks) > 0 and len(randomly_chosen_short_task_positions) < STEALING_LIMIT):
            rnd_index = random.randint(0,len(all_positions_of_short_tasks)-1)
            randomly_chosen_short_task_positions.append(all_positions_of_short_tasks.pop(rnd_index))
        randomly_chosen_short_task_positions.sort()

        #remove the selected probes from the worker queue in decreasing order of IDs
        decreasing_ids = len(randomly_chosen_short_task_positions)-1
        total_time_probes = 0
        while (decreasing_ids >= 0):
            probes_to_give.append(self.queued_probes.pop(randomly_chosen_short_task_positions[decreasing_ids]))
            total_time_probes += probes_to_give[-1][1]
            decreasing_ids -= 1
        probes_to_give = probes_to_give[::-1]  #reverse the list of tuples

        return probes_to_give



    #Worker class
    def process_next_probe_in_the_queue(self, current_time):
        global stats

        self.free_slots.pop(0)
        self.simulation.decrease_free_slots_for_load_tracking(self)
        
        if (SRPT_ENABLED):
            if (SYSTEM_SIMULATED == "Eagle"):
                pos = self.get_next_probe_acc_to_sbp_srpt(current_time)
                if (pos == -1):
                    assert (len(self.queued_probes) == 0)
                    #Cannot just return [], free_slot would not be called again
                    return [(current_time, NoopGetTaskResponseEvent(self))]
            else:
                pos = self.get_next_probe_acc_to_srpt(current_time)
        else:
            pos = 0

        job_id = self.queued_probes[pos][0]
        estimated_task_duration = self.queued_probes[pos][1]

        self.executing_big = self.simulation.jobs[job_id].job_type_for_scheduling == BIG
        if self.executing_big:
            self.queued_big                 = self.queued_big -1
            self.tstamp_start_crt_big_task  = current_time
            self.estruntime_crt_task        = estimated_task_duration
        else:
            self.tstamp_start_crt_big_task = -1
            if (self.queued_probes[0][2] == True):
               stats.STATS_SH_WAITED_FOR_BIG += 1

        was_successful, events = self.simulation.get_task(job_id, self, current_time)
        if (SYSTEM_SIMULATED != "Eagle" or not SRPT_ENABLED):
            self.queued_probes.pop(pos)
        else:
            job_bydef_big = (self.simulation.jobs[job_id].job_type_for_comparison == BIG)
            if(not was_successful or job_bydef_big):
                self.queued_probes.pop(pos)
            if(was_successful and not job_bydef_big):
                # Add estimated_delay to probes in front
                for delay_it in range(0,pos):
                    job_id        = self.queued_probes[delay_it][0]
                    new_acc_delay = self.queued_probes[delay_it][3] + estimated_task_duration
                    self.queued_probes[delay_it][3] = new_acc_delay

                    #Debug
                    job_bydef_big = self.simulation.jobs[job_id].job_type_for_comparison == BIG
                    assert(not job_bydef_big)
                    task_duration = self.simulation.jobs[job_id].estimated_task_duration
                    if (CAP_SRPT_SBP != float('inf')):
                        assert(new_acc_delay <= CAP_SRPT_SBP * task_duration), " Chosen position: %r, length %r , offending  %r" % (pos,len(self.queued_probes), new_acc_delay)
                    else:
                        assert(new_acc_delay <= CAP_SRPT_SBP and new_acc_delay >= 0), " Chosen position: %r, length %r , offending  %r" % (pos,len(self.queued_probes), new_acc_delay)

                if(self.queued_probes[pos][4]):
                    stats.STATS_STICKY_PROBES += 1

                self.queued_probes[pos][4] = True

        return events

    #Worker class
    def get_remaining_exec_time_for_job_alt(self, job_id, current_time):
        remaining_exec_time = 0
        job_info_hash       = Job.per_job_task_info[job_id]
        for task in job_info_hash:
            val = job_info_hash[task]
            if( val == -1 ):  #task not started yet
                remaining_exec_time +=  self.simulation.jobs[job_id].estimated_task_duration
            #else:             #task started but not finished
                #remaining_exec_time +=  self.simulation.jobs[job_id].estimated_task_duration - (current_time - val)
        return remaining_exec_time

    #Worker class
    def get_remaining_exec_time_for_job_dist(self, job_id, current_time):
        job = self.simulation.jobs[job_id]
        remaining_exec_time = job.remaining_exec_time
        return remaining_exec_time

    #Worker class
    def get_remaining_exec_time_for_job(self, job_id, current_time):
        job = self.simulation.jobs[job_id]
        if (len(job.unscheduled_tasks) == 0): #Improvement
            remaining_exec_time = -1
        else:
            remaining_exec_time =  len(job.unscheduled_tasks)*job.estimated_task_duration
        return remaining_exec_time

    #Worker class
    def get_next_probe_acc_to_sbp_srpt(self, current_time):
        min_remaining_exec_time = float('inf')
        position_in_queue       = -1
        estimated_delay         = 0
        chosen_is_big           = False    

        shortest_slack_in_front_short = float('inf')

        len_queue = len(self.queued_probes)
        position_it = 0
        while position_it < len_queue:
            job_id    = self.queued_probes[position_it][0]
            remaining = self.get_remaining_exec_time_for_job_dist(job_id, current_time)

            # Improvement: taking out probes of finished jobs
            if (remaining == -1):
                self.queued_probes.pop(position_it)
                len_queue = len(self.queued_probes)
            else:
                job_bydef_big = self.simulation.jobs[job_id].job_type_for_comparison == BIG
                estimated_task_duration = self.simulation.jobs[job_id].estimated_task_duration

                jump_ok = False
                if(job_bydef_big):
                    jump_ok = True
                    chosen_is_big = True
                elif(remaining < min_remaining_exec_time):
                    assert(not chosen_is_big)
                    # Check CAP jobs in front, different for short
                    jump_ok = estimated_task_duration <= shortest_slack_in_front_short

                if(jump_ok):
                    position_in_queue = position_it
                    estimated_delay = estimated_task_duration
                    min_remanining_exec_time = remaining
                    chosen_is_big = job_bydef_big
                    if (chosen_is_big):
                        break;

                    # calculate shortest slack for next probes
                if (CAP_SRPT_SBP != float('inf')):
                    slack = CAP_SRPT_SBP*estimated_task_duration - self.queued_probes[position_it][3]
                    if(CAP_SRPT_SBP == 0):
                        assert(self.queued_probes[position_it][3]==0)
                    assert (slack>=0), " offending value slack: %r CAP %r" % (slack, CAP_SRPT_SBP)

                    if(not job_bydef_big and slack < shortest_slack_in_front_short):
                        shortest_slack_in_front_short = slack
                position_it += 1

        #assert(position_in_queue>=0)

        return position_in_queue

    #Worker class
    def get_next_probe_acc_to_srpt(self, current_time):
        min_remaining_exec_time = float('inf')
        position_in_queue       = -1
        estimated_delay         = 0        

        shortest_slack_in_front = float('inf')
        for position_it in range(0,len(self.queued_probes)):
            job_id    = self.queued_probes[position_it][0]
            remaining = self.get_remaining_exec_time_for_job(job_id, current_time)
            assert(remaining >= 0)

            estimated_task_duration = self.simulation.jobs[job_id].estimated_task_duration

            if(remaining < min_remaining_exec_time):
                # Check CAP jobs in front
                if(estimated_task_duration < shortest_slack_in_front):        
                    position_in_queue = position_it
                    estimated_delay = self.simulation.jobs[job_id].estimated_task_duration
                    min_remanining_exec_time = remaining # Optimization

            # calculate shortest slack for next probes
            if (CAP_SRPT_SBP != float('inf')):
                #if(CAP_SRPT_SBP == 0):
                #    assert(self.queued_probes[position_it][3]==0)
                slack = CAP_SRPT_SBP*estimated_task_duration - self.queued_probes[position_it][3] # will be negative is estimated_task_duration = 0
                #assert (slack>=0), " offending value slack: %r CAP %r" % (slack, CAP_SRPT_SBP)
                if(slack < shortest_slack_in_front): #improvement: if its 0, no task will be allowed to bypass it
                    shortest_slack_in_front = slack


        assert(position_in_queue >= 0)
        #if(CAP_SRPT_SBP == 0):
        #    assert(position_in_queue==0), " offending value position_in_queue %r position_it %r" % (position_in_queue, position_it)
        # Add estimated_delay to probes in front        
        for delay_it in range(0,position_in_queue):
            new_acc_delay = self.queued_probes[delay_it][3] + estimated_delay            
            self.queued_probes[delay_it][3] = new_acc_delay

            #Debug
            #job_id        = self.queued_probes[delay_it][0]
            #task_duration = self.simulation.jobs[job_id].estimated_task_duration
            #if (CAP_SRPT_SBP != float('inf')):
            #    assert(new_acc_delay<=CAP_SRPT_SBP*task_duration), " Chosen position: %r , offending  %r" % (position_in_queue, new_acc_delay)
            #else:
            #    assert(new_acc_delay<=CAP_SRPT_SBP and new_acc_delay>=0), " Chosen position: %r , offending  %r" % (position_in_queue, new_acc_delay)

        return position_in_queue

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
        self.shared_cluster_status = {}

        print "self.index_last_worker_of_small_partition:         ", self.index_last_worker_of_small_partition
        print "self.index_first_worker_of_big_partition:          ", self.index_first_worker_of_big_partition

        self.small_partition_workers_hash =  {}
        self.big_partition_workers_hash = {}
        self.small_not_big_partition_workers_hash = {}

        self.small_partition_workers = self.worker_indices[:self.index_last_worker_of_small_partition+1]    # so not including the worker after :
        for node in self.small_partition_workers:
            self.small_partition_workers_hash[node] = 1

        self.big_partition_workers = self.worker_indices[self.index_first_worker_of_big_partition:]         # so including the worker before :
        for node in self.big_partition_workers:
            self.big_partition_workers_hash[node] = 1

        self.small_not_big_partition_workers = self.worker_indices[:self.index_first_worker_of_big_partition]  # so not including the worker after: 
        for node in self.small_not_big_partition_workers:
            self.small_not_big_partition_workers_hash[node] = 1

        print "Size of self.small_partition_workers_hash:         ", len(self.small_partition_workers_hash)
        print "Size of self.big_partition_workers_hash:           ", len(self.big_partition_workers_hash)
        print "Size of self.small_not_big_partition_workers_hash: ", len(self.small_not_big_partition_workers_hash)


        self.free_slots_small_partition = len(self.small_partition_workers)
        self.free_slots_big_partition = len(self.big_partition_workers)
        self.free_slots_small_not_big_partition = len(self.small_not_big_partition_workers)
 
        self.jobs_scheduled = 0
        self.jobs_completed = 0
        self.scheduled_last_job = False

        self.cluster_status_keeper = ClusterStatusKeeper()
        self.stealing_allowed = stealing_allowed
        self.SCHEDULE_BIG_CENTRALIZED = SCHEDULE_BIG_CENTRALIZED
        self.WORKLOAD_FILE = WORKLOAD_FILE
        self.btmap_tstamp = -1
        self.clusterstatus_from_btmap = None


    #Simulation class
    def subtract_sets(self, set1, set2):
        used = {}
        difference = []
        ctr_long = 0

        for item in set2:
            used[item] = 1

        for worker in set1:
            if not worker in set2:
                difference.append(worker)

        return difference


    #Simulation class
    def find_workers_random(self, probe_ratio, nr_tasks, possible_worker_indices):
        chosen_worker_indices = []
        nr_probes = max(probe_ratio*nr_tasks,MIN_NR_PROBES)
        for it in range(0,nr_probes):
            rnd_index = random.randint(0,len(possible_worker_indices)-1)
            chosen_worker_indices.append(possible_worker_indices[rnd_index])
        return chosen_worker_indices


    #Simulation class
    def find_workers_long_job_prio(self, num_tasks, estimated_task_duration, workers_queue_status, current_time, simulation, hash_workers_considered):

        chosen_worker_indices = []
        workers_needed = num_tasks
        prio_queue = Queue.PriorityQueue()

        empty_nodes = []  #performance optimization
        for index in hash_workers_considered:
            qlen          = workers_queue_status[index]                
            worker_obj    = simulation.workers[index]
            worker_id     = index

            if qlen == 0 :
                empty_nodes.append(worker_id)
                if(len(empty_nodes) == workers_needed):
                    break
            else: 
                start_of_crt_big_task = worker_obj.tstamp_start_crt_big_task
                assert(current_time >= start_of_crt_big_task)
                adjusted_waiting_time = qlen

                if(start_of_crt_big_task != -1):
                    executed_so_far = current_time - start_of_crt_big_task
                    estimated_crt_task = worker_obj.estruntime_crt_task
                    adjusted_waiting_time = 2*NETWORK_DELAY + qlen - min(executed_so_far,estimated_crt_task)

                assert adjusted_waiting_time >= 0, " offending value for adjusted_waiting_time: %r" % adjusted_waiting_time                 
                prio_queue.put((adjusted_waiting_time,worker_id))

        #performance optimization 
        if(len(empty_nodes) == workers_needed):
            return empty_nodes
        else:
            chosen_worker_indices = empty_nodes
            for nodeid in chosen_worker_indices:
                prio_queue.put((estimated_task_duration,nodeid))

        
        queue_length,worker = prio_queue.get()
        while (workers_needed > len(chosen_worker_indices)):
            next_queue_length,next_worker = prio_queue.get()
            while(queue_length <= next_queue_length and len(chosen_worker_indices) < workers_needed):
                chosen_worker_indices.append(worker)
                queue_length += estimated_task_duration

            prio_queue.put((queue_length,worker))
            queue_length = next_queue_length
            worker = next_worker 

        assert workers_needed == len(chosen_worker_indices)
        return chosen_worker_indices

    #Simulation class
    def find_workers_dlwl(self, estimated_task_durations, workers_queue_status, current_time,simulation, hash_workers_considered):

        chosen_worker_indices = []
        workers_needed = len(estimated_task_durations)
        prio_queue = Queue.PriorityQueue()

        for index in hash_workers_considered:
            qlen          = workers_queue_status[index]                
            worker_obj    = simulation.workers[index]

            adjusted_waiting_time = qlen
            start_of_crt_big_task = worker_obj.tstamp_start_crt_big_task # PAMELA: it will always be considered big for DLWL
            assert(current_time >= start_of_crt_big_task)

            if(start_of_crt_big_task != -1):
                executed_so_far = current_time - start_of_crt_big_task
                estimated_crt_task = worker_obj.estruntime_crt_task
                adjusted_waiting_time = max(2*NETWORK_DELAY + qlen - min(executed_so_far,estimated_crt_task),0)

            rand_dlwl = random.randint(0, HEARTBEAT_DELAY)
            adjusted_waiting_time += rand_dlwl
            prio_queue.put((adjusted_waiting_time,index))

        # For now all estimated task duration are the same , optimize performance
        #copy_estimated_task_durations = list(estimated_task_durations)

        queue_length,worker = prio_queue.get()
        while (workers_needed > len(chosen_worker_indices)):

            next_queue_length,next_worker = prio_queue.get()
            assert queue_length >= 0
            assert next_queue_length >= 0

            while(queue_length <= next_queue_length and len(chosen_worker_indices) < workers_needed):
                chosen_worker_indices.append(worker)
                queue_length += estimated_task_durations[0] #copy_estimated_task_durations.pop(0)

            prio_queue.put((queue_length,worker))
            queue_length = next_queue_length
            worker = next_worker 

        assert workers_needed == len(chosen_worker_indices)
        return chosen_worker_indices


    #Simulation class
    def send_probes(self, job, current_time, worker_indices, btmap):
        if SYSTEM_SIMULATED == "Hawk" or SYSTEM_SIMULATED == "IdealEagle":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "Eagle":
            return self.send_probes_eagle(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "LWL":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)
        if SYSTEM_SIMULATED == "DLWL":
            return self.send_probes_hawk(job, current_time, worker_indices, btmap)


    #Simulation class
    def send_probes_hawk(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job

        probe_events = []
        for worker_index in worker_indices:
            probe_events.append((current_time + NETWORK_DELAY, ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, btmap)))
            job.probed_workers.add(worker_index)
        return probe_events


    #Simulation class
    def try_round_of_probing(self, current_time, job, worker_list, probe_events, roundnr):
        successful_worker_indices = []
        id_worker_with_newest_btmap = -1
        freshest_btmap_tstamp = self.btmap_tstamp

        for worker_index in worker_list:
            if(not self.workers[worker_index].executing_big and not self.workers[worker_index].queued_big):
                probe_events.append((current_time + NETWORK_DELAY*(roundnr/2+1), ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, None)))
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
        non_long_job_workers = []
            
        non_long_job_workers = self.small_not_big_partition_workers[:]

        for index in self.big_partition_workers:
            if not btmap.test(index):
                non_long_job_workers.append(index)
        return non_long_job_workers

    #Simulation class
    def send_probes_eagle(self, job, current_time, worker_indices, btmap):
        self.jobs[job.id] = job
        probe_events = []

        if (job.job_type_for_scheduling == BIG):
            for worker_index in worker_indices:
                probe_events.append((current_time + NETWORK_DELAY, ProbeEvent(self.workers[worker_index], job.id, job.estimated_task_duration, job.job_type_for_scheduling, btmap)))
                job.probed_workers.add(worker_index)

        else:
            missing_probes = len(worker_indices)
            self.btmap_tstamp = -1
            self.btmap = None
            ROUNDS_SCHEDULING = 5       
 
            for i in range(0,ROUNDS_SCHEDULING):
                ok_nr, ok_nodes = self.try_round_of_probing(current_time,job,worker_indices,probe_events,i+1)  
                missing_probes -= ok_nr
                if(missing_probes == 0):
                    return probe_events
                
                list_non_long_job_workers = self.get_list_non_long_job_workers_from_btmap(self.btmap)
                worker_indices = self.find_workers_random(1, missing_probes, list_non_long_job_workers)

            if(missing_probes > 0):
                worker_indices = self.find_workers_random(1, missing_probes, self.small_not_big_partition_workers)
                self.try_round_of_probing(current_time,job,worker_indices,probe_events,ROUNDS_SCHEDULING+1)  
        
        return probe_events



    #Simulation class
    #bookkeeping for tracking the load
    def increase_free_slots_for_load_tracking(self, worker):
        self.total_free_slots += 1
        if(worker.in_small):                self.free_slots_small_partition += 1
        if(worker.in_big):                  self.free_slots_big_partition += 1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition += 1


    #Simulation class
    #bookkeeping for tracking the load
    def decrease_free_slots_for_load_tracking(self, worker):
        self.total_free_slots -= 1
        if(worker.in_small):                self.free_slots_small_partition -= 1
        if(worker.in_big):                  self.free_slots_big_partition -= 1
        if(worker.in_small_not_big):        self.free_slots_small_not_big_partition -= 1


    #Simulation class
    def get_task(self, job_id, worker, current_time):
        job = self.jobs[job_id]
        #account for the fact that this is called when the probe is launched but it needs an RTT to talk to the scheduler
        get_task_response_time = current_time + 2 * NETWORK_DELAY
        
        if len(job.unscheduled_tasks) == 0 :
            return False, [(get_task_response_time, NoopGetTaskResponseEvent(worker))]

        this_task_id=job.completed_tasks_count
        Job.per_job_task_info[job_id][this_task_id] = current_time
        events = []
        task_duration = job.unscheduled_tasks.pop()
        task_completion_time = task_duration + get_task_response_time
        print current_time, " worker:", worker.id, " task from job ", job_id, " task duration: ", task_duration, " will finish at time ", task_completion_time
        is_job_complete = job.update_task_completion_details(task_completion_time)

        if is_job_complete:
            self.jobs_completed += 1;
            print >> finished_file, task_completion_time," estimated_task_duration: ",job.estimated_task_duration, " by_def: ",job.job_type_for_comparison, " total_job_running_time: ",(job.end_time - job.start_time)

        events.append((task_completion_time, TaskEndEvent(worker, self.SCHEDULE_BIG_CENTRALIZED, self.cluster_status_keeper, job.id, job.job_type_for_scheduling, job.estimated_task_duration, this_task_id)))
        
        if SRPT_ENABLED and SYSTEM_SIMULATED == "Eagle":
                events.append((current_time + 2*NETWORK_DELAY, UpdateRemainingTimeEvent(job)))

        if len(job.unscheduled_tasks) == 0:
            logging.info("Finished scheduling tasks for job %s" % job.id)
        return True, events


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

        if(SYSTEM_SIMULATED == "DLWL"):
            self.shared_cluster_status = self.cluster_status_keeper.get_queue_status()
            self.event_queue.put((0, WorkerHeartbeatEvent(self)))

        line = self.jobs_file.readline()
        new_job = Job(self.task_distribution, line, estimate_distribution, self.off_mean_bottom, self.off_mean_top)
        self.event_queue.put((float(line.split()[0]), JobArrival(self, self.task_distribution, new_job, self.jobs_file)))
        self.jobs_scheduled = 1
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
BIG = 1
SMALL = 0

job_start_tstamps = {}

if(len(sys.argv) != 21):
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
SRPT_ENABLED                    = (sys.argv[18] == "yes")
HEARTBEAT_DELAY                 = int(sys.argv[19])
SYSTEM_SIMULATED                = sys.argv[20]  

MIN_NR_PROBES = 20 #1/100*TOTAL_WORKERS
CAP_SRPT_SBP = 5 #cap on the % of slowdown a job can tolerate for SRPT and SBP

t1 = time.time()

stats = Stats()
s = Simulation(MONITOR_INTERVAL, stealing, SCHEDULE_BIG_CENTRALIZED, WORKLOAD_FILE,CUTOFF_THIS_EXP,CUTOFF_BIG_SMALL,ESTIMATION,OFF_MEAN_BOTTOM,OFF_MEAN_TOP,TOTAL_WORKERS)
s.run()

print "Simulation ended in ", (time.time() - t1), " s "

finished_file.close()
load_file.close()

print >> stats_file, "STATS_SH_QUEUED_BEHIND_BIG:      ",        stats.STATS_SH_QUEUED_BEHIND_BIG 
print >> stats_file, "STATS_SH_EXEC_IN_BP:             ",               stats.STATS_SH_EXEC_IN_BP
print >> stats_file, "STATS_SH_WAITED_FOR_BIG:         ",           stats.STATS_SH_WAITED_FOR_BIG
print >> stats_file, "                                 "
print >> stats_file, "STATS_TOTAL_STOLEN_TASKS:        ",          stats.STATS_TOTAL_STOLEN_TASKS 
print >> stats_file, "STATS_SUCCESSFUL_STEAL_ATTEMPTS: ",   stats.STATS_SUCCESSFUL_STEAL_ATTEMPTS
print >> stats_file, "STATS_STEALING_MESSAGES:         ",           stats.STATS_STEALING_MESSAGES
print >> stats_file, "STATS_STICKY_PROBES:             ",           stats.STATS_STICKY_PROBES

stats_file.close()