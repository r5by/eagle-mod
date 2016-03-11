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

import java.util.List;




/**
 * A {@link TaskScheduler} which makes tasks instantly available for launch.
 *
 * This does not perform any resource management or queuing. It can be used for
 * applications which do not want Eagle to perform any explicit resource management
 * but still want Eagle to launch tasks.
 */
public class NoQueueTaskScheduler extends TaskScheduler {

  @Override
  int handleSubmitTaskReservation(TaskSpec taskReservation) {
    // Make this task instantly runnable
    makeTaskRunnable(taskReservation);
    return 0;
  }
  @Override
  synchronized List<TaskSpec> handleRequestTaskReservation(){
	  return null;
  }
  @Override
  int cancelTaskReservations(String requestId) {
    // Do nothing. No reservations cancelled.
    return 0;
  }

  @Override
  protected void handleTaskFinished(String requestId, String taskId) {
    // Do nothing.

  }

  @Override
  protected void handleNoTaskForReservation(TaskSpec taskSpec) {
    // Do nothing.

  }


  @Override
  int getMaxActiveTasks() {
    return -1;
  }
@Override
boolean getLongQueuedExecuting() {
	// TODO Auto-generated method stub
	return false;
}

}
