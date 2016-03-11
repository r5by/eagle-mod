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

package ch.epfl.eagle.api;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import ch.epfl.eagle.daemon.util.Network;
import ch.epfl.eagle.daemon.util.TClients;
import ch.epfl.eagle.daemon.util.TServers;
import ch.epfl.eagle.thrift.FrontendService;
import ch.epfl.eagle.thrift.IncompleteRequestException;
import ch.epfl.eagle.thrift.SchedulerService;
import ch.epfl.eagle.thrift.SchedulerService.Client;
import ch.epfl.eagle.thrift.TSchedulingRequest;
import ch.epfl.eagle.thrift.TUserGroupInfo;
//[[FLORIN
import java.util.Random;

/**
 * Java client to Eagle scheduling service. Once a client is initialize()'d it
 * can be used safely from multiple threads.
 */
public class EagleFrontendClient {
  public static boolean launchedServerAlready = false;

  private final static Logger LOG = Logger.getLogger(EagleFrontendClient.class);
  private final static int NUM_CLIENTS = 8; // Number of concurrent requests we support
  //[[FLORIN
  private final static int DEFAULT_LISTEN_PORT = new Random().nextInt(30000)+20000;

  BlockingQueue<SchedulerService.Client> clients =
      new LinkedBlockingQueue<SchedulerService.Client>();

  /**
   * Initialize a connection to an eagle scheduler.
   * @param eagleSchedulerAddr. The socket address of the Eagle scheduler.
   * @param app. The application id. Note that this must be consistent across frontends
   *             and backends.
   * @param frontendServer. A class which implements the frontend server interface (for
   *                        communication from Eagle).
   * @throws IOException
   */
  public void initialize(InetSocketAddress eagleSchedulerAddr, String app,
      FrontendService.Iface frontendServer)
      throws TException, IOException { 
    LOG.info("EagleFrontendClient using port: "+DEFAULT_LISTEN_PORT);
    initialize(eagleSchedulerAddr, app, frontendServer, DEFAULT_LISTEN_PORT);
  }

  /**
   * Initialize a connection to an eagle scheduler.
   * @param eagleSchedulerAddr. The socket address of the Eagle scheduler.
   * @param app. The application id. Note that this must be consistent across frontends
   *             and backends.
   * @param frontendServer. A class which implements the frontend server interface (for
   *                        communication from Eagle).
   * @param listenPort. The port on which to listen for request from the scheduler.
   * @throws IOException
   */
  public void initialize(InetSocketAddress eagleSchedulerAddr, String app,
      FrontendService.Iface frontendServer, int listenPort)
      throws TException, IOException {

    FrontendService.Processor<FrontendService.Iface> processor =
        new FrontendService.Processor<FrontendService.Iface>(frontendServer);

    if (!launchedServerAlready) {
      try {
        TServers.launchThreadedThriftServer(listenPort, 8, processor);
      } catch (IOException e) {
        LOG.fatal("Couldn't launch server side of frontend", e);
      }
      launchedServerAlready = true;
    }

    for (int i = 0; i < NUM_CLIENTS; i++) {
      Client client = TClients.createBlockingSchedulerClient(
    		  eagleSchedulerAddr.getAddress().getHostAddress(), eagleSchedulerAddr.getPort(),
          60000);
      clients.add(client);
    }
    clients.peek().registerFrontend(app, Network.getIPAddress(new PropertiesConfiguration())
        + ":" + listenPort);
  }

  public boolean submitJob(String app,
      List<ch.epfl.eagle.thrift.TTaskSpec> tasks, Boolean shortJob, Double estimatedDuration, TUserGroupInfo user)
          throws TException {
    return submitRequest(new TSchedulingRequest(app, tasks, shortJob, estimatedDuration, user));
  }

  public boolean submitJob(String app, List<ch.epfl.eagle.thrift.TTaskSpec> tasks,
  	  Boolean shortJob, Double estimatedDuration, TUserGroupInfo user, String description) {
  	TSchedulingRequest request = new TSchedulingRequest(app, tasks, shortJob, estimatedDuration, user);
  	request.setDescription(description);
  	return submitRequest(request);
  }

  public boolean submitJob(String app,
      List<ch.epfl.eagle.thrift.TTaskSpec> tasks, Boolean shortJob, Double estimatedDuration, TUserGroupInfo user,
      double probeRatio)
          throws TException {
    TSchedulingRequest request = new TSchedulingRequest(app, tasks, shortJob, estimatedDuration, user);
    request.setProbeRatio(probeRatio);
    return submitRequest(request);
  }

  public boolean submitRequest(TSchedulingRequest request) {
    try {
      Client client = clients.take();
      client.submitJob(request);
      clients.put(client);
    } catch (InterruptedException e) {
      LOG.fatal(e);
    } catch (IncompleteRequestException e) {
        LOG.error(e);      
    } catch (TException e) {
      LOG.error("Thrift exception when submitting job: " + e.getMessage());
      return false;
    }
    return true;
  }

  public void close() {
    for (int i = 0; i < NUM_CLIENTS; i++) {
      clients.poll().getOutputProtocol().getTransport().close();
    }
  }
}
