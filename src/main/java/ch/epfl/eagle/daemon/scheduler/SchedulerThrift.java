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

package ch.epfl.eagle.daemon.scheduler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TException;

import ch.epfl.eagle.daemon.EagleConf;
import ch.epfl.eagle.daemon.util.Network;
import ch.epfl.eagle.daemon.util.TServers;
import ch.epfl.eagle.thrift.SchedulerService;
import ch.epfl.eagle.thrift.GetTaskService;
import ch.epfl.eagle.thrift.TFullTaskId;
import ch.epfl.eagle.thrift.THostPort;
import ch.epfl.eagle.thrift.TSchedulingRequest;
import ch.epfl.eagle.thrift.TTaskLaunchSpec;

/**
 * This class extends the thrift eagle scheduler interface. It wraps the
 * {@link Scheduler} class and delegates most calls to that class.
 */
public class SchedulerThrift implements SchedulerService.Iface, GetTaskService.Iface {
  // Defaults if not specified by configuration
  public final static int DEFAULT_SCHEDULER_THRIFT_PORT = 20503;
  private final static int DEFAULT_SCHEDULER_THRIFT_THREADS = 8;
  public final static int DEFAULT_GET_TASK_PORT = 20507;

  private Scheduler scheduler = new Scheduler();

  /**
   * Initialize this thrift service.
   *
   * This spawns a multi-threaded thrift server and listens for Eagle
   * scheduler requests.
   */
  public void initialize(Configuration conf) throws IOException {
    SchedulerService.Processor<SchedulerService.Iface> processor =
        new SchedulerService.Processor<SchedulerService.Iface>(this);
    int port = conf.getInt(EagleConf.SCHEDULER_THRIFT_PORT,
        DEFAULT_SCHEDULER_THRIFT_PORT);
    int threads = conf.getInt(EagleConf.SCHEDULER_THRIFT_THREADS,
        DEFAULT_SCHEDULER_THRIFT_THREADS);
    String hostname = Network.getHostName(conf);
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    scheduler.initialize(conf, addr);
    TServers.launchThreadedThriftServer(port, threads, processor);
    int getTaskPort = conf.getInt(EagleConf.GET_TASK_PORT,
        DEFAULT_GET_TASK_PORT);
    GetTaskService.Processor<GetTaskService.Iface> getTaskprocessor =
        new GetTaskService.Processor<GetTaskService.Iface>(this);
    TServers.launchSingleThreadThriftServer(getTaskPort, getTaskprocessor);
  }

  @Override
  public boolean registerFrontend(String app, String socketAddress) {
    return scheduler.registerFrontend(app, socketAddress);
  }

  @Override
  public void submitJob(TSchedulingRequest req)
      throws TException {
    scheduler.submitJob(req);
  }

  @Override
  public void sendFrontendMessage(String app, TFullTaskId taskId,
      int status, ByteBuffer message) throws TException {
    scheduler.sendFrontendMessage(app, taskId, status, message);
  }

  @Override
  public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress, THostPort oldNodeMonitorAddress)
      throws TException {
    return scheduler.getTask(requestId, nodeMonitorAddress, oldNodeMonitorAddress);
  }
}
