/*
 * EAGLE 
 *
 * Copyright 2016 Operating Systems Laboratory EPFL
 *
 * Modified from Sparrow - University of California, Berkeley 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.eagle.daemon.nodemonitor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.lang.Thread;

import org.apache.log4j.Logger;

/**
 * This scheduler assumes that backends can execute a fixed number of tasks (equal to
 * the number of cores on the machine) and uses a FIFO queue to determine the order to launch
 * tasks whenever outstanding tasks exceed this amount.
 */
public class FifoTaskScheduler extends TaskScheduler {
  private final static Logger LOG = Logger.getLogger(FifoTaskScheduler.class);
  private NodeMonitor nodeMonitor;
  private Boolean stealing;
  private int stealingAttempts;
  public int maxActiveTasks;
  public Integer activeTasks;
  public Integer activeBigTasks;
  public Set<String> activeBigTasksRequestId;
  public LinkedBlockingQueue<TaskSpec> taskReservations =
      new LinkedBlockingQueue<TaskSpec>();
  private Integer bigTaskReservations;

  public FifoTaskScheduler(int max, NodeMonitor nodeMonitor) {
    maxActiveTasks = max;
    activeTasks = 0;
    activeBigTasks = 0;
    activeBigTasksRequestId = new HashSet<String>();
    bigTaskReservations = 0;
    this.nodeMonitor = nodeMonitor;
    this.stealing = nodeMonitor.getStealing();
    
    new Thread(""){
        public void run(){
          while(true){
		try{
			Thread.sleep(1000);
		}catch(Exception e){}
		LOG.info("Currently executing: "+activeTasks+" big: "+activeBigTasks+" queue: "+taskReservations.size()+ " big enqueued: "+ bigTaskReservations);
	  }
        }
      }.start();

  }

  @Override
  synchronized int handleSubmitTaskReservation(TaskSpec taskReservation) {
    // This method, cancelTaskReservations(), and handleTaskCompleted() are synchronized to avoid
    // race conditions between updating activeTasks and taskReservations.
    if (activeTasks < maxActiveTasks) {
      if (taskReservations.size() > 0) {
        String errorMessage = "activeTasks should be less than maxActiveTasks only " +
                              "when no outstanding reservations.";
        LOG.error(errorMessage);
        throw new IllegalStateException(errorMessage);
      }
      makeTaskRunnable(taskReservation);
      ++activeTasks;
      if (!taskReservation.shortTask){
    	  activeBigTasks++;
    	  activeBigTasksRequestId.add(taskReservation.requestId); 
    	  if(bigTaskReservations>0)
    		  bigTaskReservations--;
        LOG.debug("handleSubmitTaskReservation bigTaskReservations "+bigTaskReservations);
    	  
      }
      LOG.debug("Making task for request " + taskReservation.requestId + " runnable (" +
                activeTasks + " of " + maxActiveTasks + " task slots currently filled)");
      return 0;
    }
    LOG.debug("All " + maxActiveTasks + " task slots filled.");
    int queuedReservations = taskReservations.size();
    try {
      LOG.debug("Enqueueing task reservation with request id " + taskReservation.requestId +
                " because all task slots filled. " + queuedReservations +
                " already enqueued reservations." + " Is short task? " + taskReservation.shortTask);
      taskReservations.put(taskReservation);
      if (!taskReservation.shortTask)
      	bigTaskReservations++;
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return queuedReservations;
  }

  /* ******************************************************** 
   *********************** HAWK STEALING ******************* 
   ******************************************************** 
   */
  // Should be called by NodeMonitor , synchronized?? should be, but we dont want to block stealing until a task is done? submitTaskReservation should be the same
  @Override
  synchronized List<TaskSpec> handleRequestTaskReservation(){
	  List<TaskSpec> givenRequests = new ArrayList<TaskSpec>();
	  LOG.debug("STEALING: Handling request task reservation, queuedReservations " + taskReservations.size());
	  
	  // Test step by step: first just printing the probes to give
	  Iterator<TaskSpec> reservationsIterator = taskReservations.iterator();
	  Boolean foundFirstBig = (activeBigTasks > 0);
	  LOG.debug("Executing big task "+foundFirstBig);
	  // TODO 2 to handle more than one slot , check number of big active tasks against max
	  Boolean foundSecondBig = false, startedStealing = false;
      while (reservationsIterator.hasNext() && !foundSecondBig) {
		  TaskSpec reservation = reservationsIterator.next();
		  LOG.debug("Checking request"+ reservation.requestId+" task reservation, reservation.shortTask " + reservation.shortTask);
		  // Give only first bunch
		  if (reservation.shortTask && foundFirstBig) {
			  startedStealing =  true;
			  LOG.debug("STEALING: Adding to givenRequests request"+ reservation.requestId+" task reservation, reservation.shortTask " + reservation.shortTask);
			  // Take those probes out from queue
			  reservationsIterator.remove();
			  givenRequests.add(reservation);
		  } else if(!reservation.shortTask){
			  if(!foundFirstBig)
				  foundFirstBig = true;
			  else if (startedStealing)
				  foundSecondBig = true;
		  }
	  }
	  
	  LOG.debug("STEALING: Giving requests, lenght " + givenRequests.size()+ " FULL LIST "+taskReservations.size());	  
	  return givenRequests;
  }
  /* ******************************************************** */
  
  // EAGLE
  synchronized boolean getLongQueuedExecuting(){
	  // TODO include the ones in the queue too
	  return (activeBigTasks>0 || bigTaskReservations>0);
  }
  
  @Override
  synchronized int cancelTaskReservations(String requestId) {
    int numReservationsCancelled = 0;
    Iterator<TaskSpec> reservationsIterator = taskReservations.iterator();
    while (reservationsIterator.hasNext()) {
      TaskSpec reservation = reservationsIterator.next();
      if (reservation.requestId == requestId) {
        reservationsIterator.remove();
        ++numReservationsCancelled;
      }
    }
    return numReservationsCancelled;
  }

  @Override
  protected void handleTaskFinished(String requestId, String taskId) {
    	attemptTaskLaunch(requestId, taskId);
  }

  @Override
  protected void handleNoTaskForReservation(TaskSpec taskSpec) {
    	attemptTaskLaunch(taskSpec.previousRequestId, taskSpec.previousTaskId);
  }

  /**
   * Attempts to launch a new task.
   *
   * The parameters {@code lastExecutedRequestId} and {@code lastExecutedTaskId} are used purely
   * for logging purposes, to determine how long the node monitor spends trying to find a new
   * task to execute. This method needs to be synchronized to prevent a race condition with
   * {@link handleSubmitTaskReservation}.
   */
  private synchronized void attemptTaskLaunch(
      String lastExecutedRequestId, String lastExecutedTaskId) {
	LOG.debug("Called attemptTaskLaunch lastExecutedRequestID "+lastExecutedRequestId+" big task? "+activeBigTasksRequestId.contains(lastExecutedRequestId));
	LOG.debug("Called attemptTaskLaunch activeBigTasks "+activeBigTasks);
	if (activeBigTasksRequestId.remove(lastExecutedRequestId)){
		assert(activeBigTasks>0);
		activeBigTasks--;
	}
    TaskSpec reservation = taskReservations.poll();
    if (reservation != null) {
      reservation.previousRequestId = lastExecutedRequestId;
      reservation.previousTaskId = lastExecutedTaskId;
      makeTaskRunnable(reservation);
      if (!reservation.shortTask && activeBigTasks==0){
    	  activeBigTasks++;
    	  activeBigTasksRequestId.add(reservation.requestId);
    	  bigTaskReservations--;
    	  LOG.debug("attemptTaskLaunch: bigTaskReservations "+bigTaskReservations+" should be bigger than 0");
    	  assert(bigTaskReservations>=0);
      } 
    }else {
	  // HAWK STEALING
	  //I. HAWK Call requestTaskReservations from NodeMonitor when queue is empty
    	if(stealing){
    		LOG.debug("STEALING: FifoTaskScheduler attemptTaskLaunch, no more tasks to execute so going to STEAL ");
    		nodeMonitor.requestTaskReservations();
    	}
    	else {
    		LOG.debug("STEALING: FifoTaskScheduler attemptTaskLaunch, no STEALING: "+ stealing);    		
    	}
		
      activeTasks -= 1;
      }
  }

  @Override
  int getMaxActiveTasks() {
    return maxActiveTasks;
  }
}
