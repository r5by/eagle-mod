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

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.apache.thrift.async.AsyncMethodCallback;

import ch.epfl.eagle.daemon.util.ThriftClientPool;
import ch.epfl.eagle.thrift.InternalService;
import ch.epfl.eagle.thrift.InternalService.AsyncClient;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.cancelTaskReservations_call;
import ch.epfl.eagle.thrift.TCancelTaskReservationsRequest;
import ch.epfl.eagle.thrift.THostPort;

public class CancellationService implements Runnable {
  private ThriftClientPool<InternalService.AsyncClient> clientPool;
  private final BlockingQueue<Cancellation> cancellationQueue;
  private final static Logger LOG = Logger.getLogger(CancellationService.class);

  private class Cancellation {
    public String requestId;
    public InetSocketAddress nodeMonitorAddress;

    public Cancellation (String requestId, InetSocketAddress nodeMonitorAddress) {
      this.requestId = requestId;
      this.nodeMonitorAddress = nodeMonitorAddress;
    }
  }

  public CancellationService(ThriftClientPool<InternalService.AsyncClient> clientPool) {
    this.clientPool = clientPool;
    this.cancellationQueue = new LinkedBlockingQueue<Cancellation>();
  }

  public void addCancellation(String requestId, THostPort nodeMonitorAddress) {
    InetSocketAddress socketAddress = new InetSocketAddress(
        nodeMonitorAddress.host, nodeMonitorAddress.port);
    this.cancellationQueue.add(new Cancellation(requestId, socketAddress));
  }

  public void run() {
    while (true) {
      Cancellation cancellation = null;
      try {
        cancellation = cancellationQueue.take();
      } catch (InterruptedException e) {
        LOG.fatal(e);
      }

      try {
        InternalService.AsyncClient client = clientPool.borrowClient(
            cancellation.nodeMonitorAddress);
        LOG.debug("Cancelling tasks for request " + cancellation.requestId + " on node " +
            cancellation.nodeMonitorAddress);
        client.cancelTaskReservations(
            new TCancelTaskReservationsRequest(cancellation.requestId),
            new CancelTaskReservationsCallback(cancellation.nodeMonitorAddress));
      } catch (Exception e) {
        LOG.error("Error cancelling request " + cancellation.requestId + " on node " +
                  cancellation.nodeMonitorAddress+ ": " + e.getMessage());
      }
    }
  }

  /** A callback for CancelTaskReservations() RPCs that returns the client to the client pool. */
  private class CancelTaskReservationsCallback
  implements AsyncMethodCallback<cancelTaskReservations_call> {
    InetSocketAddress nodeMonitorAddress;

    public CancelTaskReservationsCallback(InetSocketAddress nodeMonitorAddress) {
      this.nodeMonitorAddress = nodeMonitorAddress;
    }

    @Override
    public void onComplete(cancelTaskReservations_call response) {
      try {
        clientPool.returnClient(nodeMonitorAddress, (AsyncClient) response.getClient());
      } catch (Exception e) {
        LOG.error("Error returning client to node monitor client pool: " + e);
      }
    }

    @Override
    public void onError(Exception exception) {
      LOG.error("Error executing cancelTaskReservations RPC: " + exception);
    }

  }

}
