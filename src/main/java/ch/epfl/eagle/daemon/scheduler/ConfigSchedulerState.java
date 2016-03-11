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
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

import ch.epfl.eagle.daemon.EagleConf;
import ch.epfl.eagle.daemon.util.ConfigUtil;

/**
 * Scheduler state that operates based on a static configuration file.
 */
public class ConfigSchedulerState implements SchedulerState {
  private static final Logger LOG = Logger.getLogger(ConfigSchedulerState.class);

  Set<InetSocketAddress> backends;
  private Configuration conf;

  @Override
  public void initialize(Configuration conf) {
    backends = ConfigUtil.parseBackends(conf);
    this.conf = conf;
  }

  @Override
  public boolean watchApplication(String appId) {
   //[[FLORIN
   /* if (!appId.equals(conf.getString(EagleConf.STATIC_APP_NAME))) {
      LOG.warn("Requested watch for app " + appId +
          " but was expecting app " + conf.getString(EagleConf.STATIC_APP_NAME));
    }*/
    return true;
  }

  @Override
  public Set<InetSocketAddress> getBackends(String appId) {
   //[[FLORIN
    /*if (!appId.equals(conf.getString(EagleConf.STATIC_APP_NAME))) {
     LOG.warn("Requested backends for app " + appId +
          " but was expecting app " + conf.getString(EagleConf.STATIC_APP_NAME));
    }*/
    return backends;
  }

}
