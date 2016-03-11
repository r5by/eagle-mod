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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import ch.epfl.eagle.daemon.util.Logging;
import ch.epfl.eagle.daemon.util.Network;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsRequest;
import ch.epfl.eagle.thrift.TFullTaskId;
import ch.epfl.eagle.thrift.THostPort;
import ch.epfl.eagle.thrift.TTaskLaunchSpec;
import ch.epfl.eagle.thrift.TUserGroupInfo;

/**
 * A TaskScheduler is a buffer that holds task reservations until an application backend is
 * available to run the task. When a backend is ready, the TaskScheduler requests the task
 * from the {@link Scheduler} that submitted the reservation.
 *
 * Each scheduler will implement a different policy determining when to launch tasks.
 *
 * Schedulers are required to be thread safe, as they will be accessed concurrently from
 * multiple threads.
 */
public abstract class TaskScheduler {
  protected class TaskSpec {
    public String appId;
    public Boolean shortTask;
    public TUserGroupInfo user;
    public String requestId;
    public THostPort originalNodeMonitorAddress;
    
    public InetSocketAddress schedulerAddress;
    public InetSocketAddress appBackendAddress;

    /**
     * ID of the task that previously ran in the slot this task is using. Used
     * to track how long it takes to fill an empty slot on a slave. Empty if this task was launched
     * immediately, because there were empty slots available on the slave.  Filled in when
     * the task is launched.
     */
    public String previousRequestId;
    public String previousTaskId;

    /** Filled in after the getTask() RPC completes. */
    public TTaskLaunchSpec taskSpec;

    public TaskSpec(TEnqueueTaskReservationsRequest request, InetSocketAddress appBackendAddress) {
      appId = request.getAppId();
      user = request.getUser();
      shortTask = request.shortTask;
      requestId = request.getRequestId();
      originalNodeMonitorAddress = request.originalNodeMonitorAddress;
      schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(),
                                               request.getSchedulerAddress().getPort());
      this.appBackendAddress = appBackendAddress;
      previousRequestId = "";
      previousTaskId = "";
    }
  }

  private final static Logger LOG = Logger.getLogger(TaskScheduler.class);
  private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);
  private String ipAddress;

  protected Configuration conf;
  private final BlockingQueue<TaskSpec> runnableTaskQueue =
      new LinkedBlockingQueue<TaskSpec>();

  /** Initialize the task scheduler, passing it the current available resources
   *  on the machine. */
  void initialize(Configuration conf, int nodeMonitorPort) {
    this.conf = conf;
    this.ipAddress = Network.getIPAddress(conf);
  }

  /**
   * Get the next task available for launching. This will block until a task is available.
   */
  TaskSpec getNextTask() {
    TaskSpec task = null;
    try {
      task = runnableTaskQueue.take();
    } catch (InterruptedException e) {
      LOG.fatal(e);
    }
    return task;
  }

  /**
   * Returns the current number of runnable tasks (for testing).
   */
  int runnableTasks() {
    return runnableTaskQueue.size();
  }

  void tasksFinished(List<TFullTaskId> finishedTasks) {
    for (TFullTaskId t : finishedTasks) {
      AUDIT_LOG.info(Logging.auditEventString("task_completed", t.getRequestId(), t.getTaskId()));
      handleTaskFinished(t.getRequestId(), t.getTaskId());
    }
  }

  void noTaskForReservation(TaskSpec taskReservation) {
    AUDIT_LOG.info(Logging.auditEventString("node_monitor_get_task_no_task",
                                            taskReservation.requestId,
                                            taskReservation.previousRequestId,
                                            taskReservation.previousTaskId));
    handleNoTaskForReservation(taskReservation);
  }

  protected void makeTaskRunnable(TaskSpec task) {
    try {
      LOG.debug("Putting reservation for request " + task.requestId + " in runnable queue");
      runnableTaskQueue.put(task);
    } catch (InterruptedException e) {
      LOG.fatal("Unable to add task to runnable queue: " + e.getMessage());
    }
  }

  public synchronized void submitTaskReservations(TEnqueueTaskReservationsRequest request,
                                                  InetSocketAddress appBackendAddress) {
    for (int i = 0; i < request.getNumTasks(); ++i) {
      LOG.debug("Creating reservation " + i + " for request " + request.getRequestId() + " shortTask " + request.shortTask);
      TaskSpec reservation = new TaskSpec(request, appBackendAddress);
      int queuedReservations = handleSubmitTaskReservation(reservation);
      AUDIT_LOG.info(Logging.auditEventString("reservation_enqueued", ipAddress, request.requestId,
                                              queuedReservations));
    }
  }
  abstract boolean getLongQueuedExecuting();
  
  // TASK SCHEDULERS MUST IMPLEMENT THE FOLLOWING.

  /**
   * Handles a task reservation. Returns the number of queued reservations.
   */
  abstract int handleSubmitTaskReservation(TaskSpec taskReservation);

  /* ******************************************************** 
   *********************** HAWK STEALING ******************* 
   ******************************************************** 
   */
  abstract List<TaskSpec> handleRequestTaskReservation();

  /**
   * Cancels all task reservations with the given request id. Returns the number of task
   * reservations cancelled.
   */
  abstract int cancelTaskReservations(String requestId);

  /**
   * Handles the completion of a task that has finished executing.
   */
  protected abstract void handleTaskFinished(String requestId, String taskId);

  /**
   * Handles the case when the node monitor tried to launch a task for a reservation, but
   * the corresponding scheduler didn't return a task (typically because all of the corresponding
   * job's tasks have been launched).
   */
  protected abstract void handleNoTaskForReservation(TaskSpec taskSpec);

  /**
   * Returns the maximum number of active tasks allowed (the number of slots).
   *
   * -1 signals that the scheduler does not enforce a maximum number of active tasks.
   */
  abstract int getMaxActiveTasks();
}
