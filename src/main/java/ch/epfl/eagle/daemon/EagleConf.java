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

/**
 * Configuration parameters for eagle.
 */
public class EagleConf {
  // Values: "debug", "info", "warn", "error", "fatal"
  public final static String LOG_LEVEL = "log_level";

  public final static String SCHEDULER_THRIFT_PORT = "scheduler.thrift.port";
  public final static String SCHEDULER_THRIFT_THREADS =
      "scheduler.thrift.threads";
  // Listen port for the state store --> scheduler interface
  public final static String SCHEDULER_STATE_THRIFT_PORT = "scheduler.state.thrift.port";
  public final static String SCHEDULER_STATE_THRIFT_THREADS =
      "scheduler.state.thrift.threads";

  /**
   * Whether the scheduler should cancel outstanding reservations when all of a job's tasks have
   * been scheduled.  Should be set to "true" or "false".
   */
  public final static String CANCELLATION = "cancellation";
  public final static boolean DEFAULT_CANCELLATION = true;

  /* List of ports corresponding to node monitors (backend interface) this daemon is
   * supposed to run. In most deployment scenarios this will consist of a single port,
   * or will be left unspecified in favor of the default port. */
  public final static String NM_THRIFT_PORTS = "agent.thrift.ports";

  /* List of ports corresponding to node monitors (internal interface) this daemon is
   * supposed to run. In most deployment scenarios this will consist of a single port,
   * or will be left unspecified in favor of the default port. */
  public final static String INTERNAL_THRIFT_PORTS = "internal_agent.thrift.ports";

  public final static String NM_THRIFT_THREADS = "agent.thrift.threads";
  public final static String INTERNAL_THRIFT_THREADS =
      "internal_agent.thrift.threads";
  /** Type of task scheduler to use on node monitor. Values: "fifo," "round_robin, " "priority." */
  public final static String NM_TASK_SCHEDULER_TYPE = "node_monitor.task_scheduler";

  public final static String SYSTEM_MEMORY = "system.memory";
  public final static int DEFAULT_SYSTEM_MEMORY = 1024;

  public final static String SYSTEM_CPUS = "system.cpus";
  public final static int DEFAULT_SYSTEM_CPUS = 4;

  // Values: "standalone", "configbased." Only "configbased" works currently.
  public final static String DEPLOYMENT_MODE = "deployment.mode";
  public final static String DEFAULT_DEPLOYMENT_MODE = "production";

  /** HAWK */
  // Values: "true","false".
  public final static String STEALING = "nodemonitor.stealing";
  public final static Boolean DEFAULT_STEALING = true;
  // Values: integer > 0.
  public final static String STEALING_ATTEMPTS = "nodemonitor.stealing_attempts";
  public final static int DEFAULT_STEALING_ATTEMPTS = 1;
  // Values: from 0 to 100
  public final static String SMALL_PARTITION = "small.partition";
  public final static int DEFAULT_SMALL_PARTITION = 10;
  public final static String BIG_PARTITION = "big.partition";
  public final static int DEFAULT_BIG_PARTITION = 90;
  // Use default big duration values for testing purposes
  public final static String USE_GIVEN_DURATIONS = "hawk.use_given_durations";
  public final static Boolean DEFAULT_USE_GIVEN_DURATIONS = false;
  public final static String GIVEN_BIG_DURATIONS = "hawk.big_job_duration";
  public final static Double DEFAULT_GIVEN_BIG_DURATIONS = 10.0;
 
  /** EAGLE */
  public final static String PIGGYBACKING = "eagle.piggybacking";
  public final static Boolean DEFAULT_PIGGYBACKING = false;
  public final static String GOSSIPING = "eagle.gossiping";
  public final static Boolean DEFAULT_GOSSIPING = false;
  public final static String RETRY_ROUNDS = "eagle.retry_rounds";
  public final static int DEFAULT_RETRY_ROUNDS = 1;
  public final static String LAST_ROUND_SHORT_PARTITION = "eagle.last_round_short_partition";
  public final static Boolean DEFAULT_LAST_ROUND_SHORT_PARTITION = true;  
  
  /** The ratio of probes used in a scheduling decision to tasks. */
  // For requests w/o constraints...
  public final static String SAMPLE_RATIO = "sample.ratio";
  public final static double DEFAULT_SAMPLE_RATIO = 1.05;
  // For requests w/ constraints...
  public final static String SAMPLE_RATIO_CONSTRAINED = "sample.ratio.constrained";
  public final static int DEFAULT_SAMPLE_RATIO_CONSTRAINED = 2;

  /** The hostname of this machine. */
  public final static String HOSTNAME = "hostname";
  /**
   * Use an invalid default value, so that the "special task set" is only used by those
   * who know what they're doing.
   */
  public final static int DEFAULT_SPECIAL_TASK_SET_SIZE = -1;

  // Parameters for static operation.
  /** Expects a comma-separated list of host:port pairs describing the address of the
    * internal interface of the node monitors. */
  public final static String STATIC_NODE_MONITORS = "static.node_monitors";
  public final static String STATIC_APP_NAME = "static.app.name";

  public static final String GET_TASK_PORT = "get_task.port";
  
  public final static String SPREAD_EVENLY_TASK_SET_SIZE = "spread_evenly_task_set_size";
  public final static int DEFAULT_SPREAD_EVENLY_TASK_SET_SIZE = 1;
}
