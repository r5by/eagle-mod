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

package ch.epfl.eagle.daemon;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import ch.epfl.eagle.daemon.nodemonitor.StandaloneNodeMonitorState;
import ch.epfl.eagle.daemon.scheduler.StandaloneSchedulerState;

/**
 * When Eagle is running in standalone mode (single machine) it is
 * necessary to have a singleton global state store to coordinate data
 * between {@link StandaloneNodeMonitorState} and
 * {@link StandaloneSchedulerState}. This class acts as that state store.
 */
public class StandaloneStateStore {
  private static final StandaloneStateStore instance =
      new StandaloneStateStore();

  public static StandaloneStateStore getInstance() {
          return instance;
  }

  // appId -> map of app nodes
  private Map<String, Set<InetSocketAddress>> applications;

  // Private constructor prevents instantiation from other classes
  private StandaloneStateStore() {
    applications = new HashMap<String, Set<InetSocketAddress>>();
  }

  // SOURCE: StandaloneNodeMonitorState
  public synchronized void registerBackend(
      String appId, InetSocketAddress nmAddr) {
    if (!this.applications.containsKey(appId)) {
      this.applications.put(appId, new HashSet<InetSocketAddress>());
    }
    this.applications.get(appId).add(nmAddr);
  }

  // SOURCE: StandaloneSchedulerState
  public synchronized Set<InetSocketAddress> getBackends(
      String appId) {
    if (applications.containsKey(appId)) {
      return new HashSet<InetSocketAddress>(
          applications.get(appId));
    } else {
      return new HashSet<InetSocketAddress>();
    }
  }
}
