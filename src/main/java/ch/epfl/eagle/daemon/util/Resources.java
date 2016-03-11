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

package ch.epfl.eagle.daemon.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.Configuration;

import ch.epfl.eagle.daemon.EagleConf;

/** Utilities for interrogating system resources. */
public class Resources {
  public static int getSystemMemoryMb(Configuration conf) {
    int systemMemory = -1;
    try {
      Process p = Runtime.getRuntime().exec("cat /proc/meminfo");  
      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = in.readLine();
      while (line != null) {
        if (line.contains("MemTotal")) { 
          String[] parts = line.split("\\s+");
          if (parts.length > 1) {
            int memory = Integer.parseInt(parts[1]) / 1000;
            systemMemory = memory;
          }
        }
        line = in.readLine();
      }
    } catch (IOException e) {}
    if (conf.containsKey(EagleConf.SYSTEM_MEMORY)) {
      return conf.getInt(EagleConf.SYSTEM_MEMORY);
    } else {
      if (systemMemory != -1) {
        return systemMemory;
      } else {
        return EagleConf.DEFAULT_SYSTEM_MEMORY;
      }
    }
  }
  
  public static int getSystemCPUCount(Configuration conf) {
    // No system interrogation yet
    return conf.getInt(EagleConf.SYSTEM_CPUS, EagleConf.DEFAULT_SYSTEM_CPUS);
  }
}
