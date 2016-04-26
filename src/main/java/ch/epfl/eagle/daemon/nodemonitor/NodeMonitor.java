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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;


import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ch.epfl.eagle.daemon.EagleConf;
import ch.epfl.eagle.daemon.nodemonitor.TaskScheduler.TaskSpec;
import ch.epfl.eagle.daemon.scheduler.TaskPlacer;
import ch.epfl.eagle.daemon.scheduler.UnconstrainedTaskPlacer;
import ch.epfl.eagle.daemon.util.Logging;
import ch.epfl.eagle.daemon.util.Network;
import ch.epfl.eagle.daemon.util.Resources;
import ch.epfl.eagle.daemon.util.ThriftClientPool;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.receiveGossip_call;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.sendTasksReservations_call;
import ch.epfl.eagle.thrift.SchedulerService;
import ch.epfl.eagle.thrift.SchedulerService.AsyncClient;
import ch.epfl.eagle.thrift.SchedulerService.AsyncClient.sendFrontendMessage_call;
import ch.epfl.eagle.thrift.InternalService;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsRequest;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsResponse;
import ch.epfl.eagle.thrift.TFullTaskId;
import ch.epfl.eagle.thrift.THostPort;

/**
 * A Node Monitor which is responsible for communicating with application
 * backends. This class is wrapped by multiple thrift servers, so it may be
 * concurrently accessed when handling multiple function calls simultaneously.
 */
public class NodeMonitor {
	private final static Logger LOG = Logger.getLogger(NodeMonitor.class);
	private final static Logger AUDIT_LOG = Logging.getAuditLogger(TaskScheduler.class);

	private static NodeMonitorState state;
	private HashMap<String, InetSocketAddress> appSockets = new HashMap<String, InetSocketAddress>();
	private HashMap<String, List<TFullTaskId>> appTasks = new HashMap<String, List<TFullTaskId>>();
	/** HAWK STEALING: Thrift client pool for communicating with node monitors */
	ThriftClientPool<InternalService.AsyncClient> nodeMonitorClientPool = new ThriftClientPool<InternalService.AsyncClient>(
			new ThriftClientPool.InternalServiceMakerFactory());
	private Boolean stealing;
	private int maxStealingAttempts;
	private int stealingAttempts;
	public double smallPartition = 0;
	public double bigPartition = 0;
	/** EAGLE PIGGYBACKING */
	private List<String> notExecutingLong;
	private double longStatusTimestamp;
	private boolean gossiping;

	// Map to scheduler socket address for each request id.
	private ConcurrentMap<String, InetSocketAddress> requestSchedulers = Maps.newConcurrentMap();
	private ThriftClientPool<SchedulerService.AsyncClient> schedulerClientPool = new ThriftClientPool<SchedulerService.AsyncClient>(
			new ThriftClientPool.SchedulerServiceMakerFactory());
	private TaskScheduler scheduler;
	private TaskLauncherService taskLauncherService;
	private String ipAddress;

	public void initialize(Configuration conf, int nodeMonitorInternalPort) throws UnknownHostException {
		String mode = conf.getString(EagleConf.DEPLOYMENT_MODE, "unspecified");
		stealing = conf.getBoolean(EagleConf.STEALING, EagleConf.DEFAULT_STEALING);
		maxStealingAttempts = conf.getInt(EagleConf.STEALING_ATTEMPTS, EagleConf.DEFAULT_STEALING_ATTEMPTS);
		smallPartition = conf.getInt(EagleConf.SMALL_PARTITION, EagleConf.DEFAULT_SMALL_PARTITION);
		bigPartition = conf.getInt(EagleConf.BIG_PARTITION, EagleConf.DEFAULT_BIG_PARTITION);
		gossiping = conf.getBoolean(EagleConf.GOSSIPING, EagleConf.DEFAULT_GOSSIPING);

		stealingAttempts = 0;
		LOG.info("STEALING set to : " + stealing);
		if (mode.equals("standalone")) {
			state = new StandaloneNodeMonitorState();
		} else if (mode.equals("configbased")) {
			state = new ConfigNodeMonitorState();
		} else {
			throw new RuntimeException("Unsupported deployment mode: " + mode);
		}
		try {
			state.initialize(conf);
		} catch (IOException e) {
			LOG.fatal("Error initializing node monitor state.", e);
		}

		longStatusTimestamp = -1;
		// At the beginning all nodes will be free from Long jobs
		notExecutingLong = new ArrayList<String>();
		List<InetSocketAddress> nodeList = Lists.newArrayList(state.getNodeMonitors());
		// TODO EAGLE add itself to the list
		for (InetSocketAddress isa : nodeList)
			notExecutingLong.add(isa.toString());

		int mem = Resources.getSystemMemoryMb(conf);
		LOG.info("Using memory allocation: " + mem);

		ipAddress = Network.getIPAddress(conf);

		int cores = Resources.getSystemCPUCount(conf);
		LOG.info("Using core allocation: " + cores);

		String task_scheduler_type = conf.getString(EagleConf.NM_TASK_SCHEDULER_TYPE, "fifo");
		LOG.info("Task scheduler type: " + task_scheduler_type);
		if (task_scheduler_type.equals("round_robin")) {
			scheduler = new RoundRobinTaskScheduler(cores);
		} else if (task_scheduler_type.equals("fifo")) {
			scheduler = new FifoTaskScheduler(cores, this);
		} else if (task_scheduler_type.equals("priority")) {
			scheduler = new PriorityTaskScheduler(cores);
		} else {
			throw new RuntimeException("Unsupported task scheduler type: " + mode);
		}
		scheduler.initialize(conf, nodeMonitorInternalPort);
		taskLauncherService = new TaskLauncherService();
		taskLauncherService.initialize(conf, scheduler, nodeMonitorInternalPort);
	}

	/**
	 * Registers the backend with assumed 0 load, and returns true if successful.
	 * Returns false if the backend was already registered.
	 */
	public boolean registerBackend(String appId, InetSocketAddress nmAddr, InetSocketAddress backendAddr) {
		LOG.info("Registering app " + appId);
		LOG.info(Logging.functionCall(appId, nmAddr, backendAddr));
		if (appSockets.containsKey(appId)) {
			LOG.info("Attempt to re-register app " + appId);
			return false;
		}
		appSockets.put(appId, backendAddr);
		appTasks.put(appId, new ArrayList<TFullTaskId>());
		return state.registerBackend(appId, nmAddr);
	}

	/**
	 * Account for tasks which have finished.
	 */
	public void tasksFinished(List<TFullTaskId> tasks) {
		LOG.debug(System.currentTimeMillis() + " " + Logging.functionCall(tasks));
		scheduler.tasksFinished(tasks);
	}

	/* ********************************************************
	 * ********************** HAWK STEALING *******************
	 * *******************************************************
	 */
	// II. HAWK Method for asking probes
	private List<InetSocketAddress> getCleanWorkersList(){
		Set<InetSocketAddress> backends = state.getNodeMonitors();
		List<InetSocketAddress> listBackends = new ArrayList<InetSocketAddress>(backends);
		/*try {
			//String inetAddress = InetAddress.getLocalHost().getHostAddress();
			// Take out this monitor from backends
			int i = 0;
			for (InetSocketAddress backend : backends) {
				if (backend.getAddress().getHostAddress().equals(ipAddress)) //|| backend.getAddress().getHostAddress().equals(inetAddress))
					break;				
				i++;
			}
			//LOG.debug("STEALING: Going to search "+ ipAddress + " or "+ inetAddress + " in " + listBackends+ " index "+ i);
			//assert(i<listBackends.size());
			listBackends.remove(i);
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}*/
		// Order by id
		Collections.sort(listBackends, new Comparator<InetSocketAddress>() {
			public int compare(InetSocketAddress a1, InetSocketAddress a2) {
				return a1.toString().compareTo(a2.toString());
			}
		});	
		return listBackends;
	}
	
	public void requestTaskReservations() {
		LOG.info(Logging.functionCall());
		// 1. call sendTaskReservations to n workers
		List<InetSocketAddress> listBackends = getCleanWorkersList();
		// Get the big partition
		LOG.debug("STEALING: Initial node list size: " + listBackends.size());
		int last_nodeID = (int) (listBackends.size() * bigPartition / 100);
		listBackends = listBackends.subList(0, last_nodeID);
		LOG.debug("STEALING: Using nodes from 0 to " + last_nodeID + ". List consists of : " + listBackends);

		LOG.debug("STEALING: New list of backends " + listBackends.toString());
		Collections.shuffle(listBackends);
		InetSocketAddress chosenBackend = listBackends.get(0);

		try {
			InternalService.AsyncClient client = nodeMonitorClientPool.borrowClient(chosenBackend);
			stealingAttempts++;
			LOG.debug("STEALING: Launching sendTasksReservations on node: " + chosenBackend + " stealing attempts" + stealingAttempts);
			client.sendTasksReservations(new SendTaskReservationsCallback(chosenBackend, client));
			LOG.debug("STEALING: Finished launching sendTasksReservations on node: " + chosenBackend);
		} catch (Exception e) {
			LOG.error("Error enqueuing task on node " + chosenBackend.toString() + ":" + e);
		}

	}

	// III. HAWK Method for sending probes
	public List<TEnqueueTaskReservationsRequest> sendTaskReservations() {
		LOG.info(Logging.functionCall());
		// ask scheduler for task reservations
		List<TaskSpec> requestsList = scheduler.handleRequestTaskReservation();
		List<TEnqueueTaskReservationsRequest> reservationsList = new ArrayList<TEnqueueTaskReservationsRequest>();
		// Convert from taskspec list to TEnqueueTaskReservationsRequest
		for (TaskSpec request : requestsList) {
			THostPort schedulerAddress = Network.socketAddressToThrift(request.schedulerAddress);
			TEnqueueTaskReservationsRequest reservation = new TEnqueueTaskReservationsRequest(request.appId, request.user, request.shortTask, request.requestId,
					schedulerAddress, request.originalNodeMonitorAddress, 1);
			reservationsList.add(reservation);
		}
		return reservationsList;
	}

	/**
	 * Callback for requestTaskReservations() that returns the client to the
	 * client pool.
	 */
	private class SendTaskReservationsCallback implements AsyncMethodCallback<sendTasksReservations_call> {
		private InetSocketAddress nodeMonitorSocket;
		private InternalService.AsyncClient client;

		public SendTaskReservationsCallback(InetSocketAddress socket, InternalService.AsyncClient client) {
			nodeMonitorSocket = socket;
			this.client = client;
		}

		public void onError(Exception exception) {
			try {
				nodeMonitorClientPool.returnClient(nodeMonitorSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
			LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
		}

		@Override
		public void onComplete(sendTasksReservations_call response) {
			try {
				List<TEnqueueTaskReservationsRequest> taskReservationRequests = response.getResult();
				// TODO check for null
				LOG.debug("STEALING: SendTaskReservationsCallback, answer got size " + taskReservationRequests.size());
				// 2. enqueue task reservations
				for (TEnqueueTaskReservationsRequest taskReservationRequest : taskReservationRequests) {
					LOG.debug("STEALING: enqueuing taskReservationRequest id " + taskReservationRequest.getRequestId() + " originalNodeMonitorAddress "
							+ taskReservationRequest.originalNodeMonitorAddress);
					enqueueTaskReservations(taskReservationRequest);
				}
				nodeMonitorClientPool.returnClient(nodeMonitorSocket, client);
				// IV. Stealing policy Retry
				if (stealingAttempts < maxStealingAttempts && (taskReservationRequests.size() == 0)) {
					LOG.debug("STEALING: Got 0 probes. stealingAttempts" + stealingAttempts + " maxStealingAttempts " + maxStealingAttempts);
					requestTaskReservations();
				}
			} catch (Exception e) {
				LOG.error(e);
			}
		}
	}


	/* ********************************************************
	 * ********************** EAGLE GOSSIPING ****************
	 * *******************************************************
	 */
	// II. Gossip
	private void gossipNotExecutingLong(int round){
		List<InetSocketAddress> listBackends = getCleanWorkersList();
		// Choose randomly log(n) workers
  		Collections.shuffle(listBackends);
      int gossip_fanout = (int) (Math.ceil(Math.log(listBackends.size())));
      for(int i=0; i<gossip_fanout; i++){
      	InetSocketAddress chosenBackend = listBackends.get(i);
    		try {
    			InternalService.AsyncClient client = nodeMonitorClientPool.borrowClient(chosenBackend);
    			LOG.debug("STEALING: Launching gossipNotExecutingLong on node: " + chosenBackend + " round " + round);
    			client.receiveGossip(notExecutingLong, longStatusTimestamp, round, new ReceiveGossipCallback(chosenBackend, client));
    		} catch (Exception e) {
    			LOG.error("Error enqueuing task on node " + chosenBackend.toString() + ":" + e);
    		}
      }
	}

	/**
	 * Callback for receiveGossip() that returns the client to the
	 * client pool.
	 */
	private class ReceiveGossipCallback implements AsyncMethodCallback<receiveGossip_call> {
		private InetSocketAddress nodeMonitorSocket;
		private InternalService.AsyncClient client;

		public ReceiveGossipCallback(InetSocketAddress socket, InternalService.AsyncClient client) {
			nodeMonitorSocket = socket;
			this.client = client;
		}

		public void onError(Exception exception) {
			try {
				nodeMonitorClientPool.returnClient(nodeMonitorSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
			LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
		}
	
		@Override
		public void onComplete(receiveGossip_call response) {
			try {
				LOG.debug("Gossip completed " );
				nodeMonitorClientPool.returnClient(nodeMonitorSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
		}
	}
	
	// III. Receive gossip
	public void receiveGossip(List<String> notExecutingLong, double longStatusTimestamp, int round){
		if (this.longStatusTimestamp<longStatusTimestamp){
			this.longStatusTimestamp = longStatusTimestamp;
			this.notExecutingLong = notExecutingLong;
			//TODO CHANGE round to conf or something
			int max_rounds = 3;
			round = round++;
			if(round<=max_rounds){
				gossipNotExecutingLong(round);
			}
		}
	}
	
	/* *******************************************************
	 * EAGLE : piggybacking nodes NOT executing long together with probes
	 * ******************************************************
	 */
	// I. EAGLE Getting long status
	public boolean enqueueTasksCentralized(TEnqueueTaskReservationsRequest request, List<String> notExecutingLong, double longStatusTimestamp) {
		assert (notExecutingLong.size() > 0);
		LOG.info("EAGLE Received enqueue task from CENTRALIZED " + ipAddress + " longStatusTimestamp " + longStatusTimestamp + ", count not executing long "
				+ notExecutingLong.size());
		this.longStatusTimestamp = longStatusTimestamp;
		this.notExecutingLong = notExecutingLong;
		if(gossiping)
			gossipNotExecutingLong(1);
		return enqueueTaskReservations(request);
	}

	public TEnqueueTaskReservationsResponse probeEnqueueTaskReservations(TEnqueueTaskReservationsRequest request) {
		boolean response = enqueueTaskReservations(request);
		LOG.debug("EAGLE probeEnqueueTaskReservations executing long: " + scheduler.getLongQueuedExecuting() + " longStatusTimestamp " + longStatusTimestamp + " notExecutingLong " + notExecutingLong);
		
		return new TEnqueueTaskReservationsResponse(response,scheduler.getLongQueuedExecuting(),notExecutingLong,longStatusTimestamp);
	}

	public boolean enqueueTaskReservations(TEnqueueTaskReservationsRequest request) {
		LOG.debug(Logging.functionCall(request));
		stealingAttempts = 0;
		AUDIT_LOG.info(Logging.auditEventString("node_monitor_enqueue_task_reservation", ipAddress, request.requestId));
		LOG.info("Received enqueue task reservation request from " + ipAddress + " for request " + request.requestId + " shortTask " + request.shortTask);

		InetSocketAddress schedulerAddress = new InetSocketAddress(request.getSchedulerAddress().getHost(), request.getSchedulerAddress().getPort());
		requestSchedulers.put(request.getRequestId(), schedulerAddress);

		InetSocketAddress socket = appSockets.get(request.getAppId());
		if (socket == null) {
			LOG.error("No socket stored for " + request.getAppId() + " (never registered?). " + "Can't launch task.");
			return false;
		}
		scheduler.submitTaskReservations(request, socket);
		return true;
	}

	public void cancelTaskReservations(String requestId) {
		int numReservationsCancelled = scheduler.cancelTaskReservations(requestId);
		AUDIT_LOG.debug(Logging.auditEventString("node_monitor_cancellation", ipAddress, requestId, numReservationsCancelled));
	}

	private class sendFrontendMessageCallback implements AsyncMethodCallback<sendFrontendMessage_call> {
		private InetSocketAddress frontendSocket;
		private AsyncClient client;

		public sendFrontendMessageCallback(InetSocketAddress socket, AsyncClient client) {
			frontendSocket = socket;
			this.client = client;
		}

		public void onComplete(sendFrontendMessage_call response) {
			try {
				schedulerClientPool.returnClient(frontendSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
		}

		public void onError(Exception exception) {
			try {
				schedulerClientPool.returnClient(frontendSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
			LOG.error(exception);
		}
	}

	public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) {
		LOG.debug(Logging.functionCall(app, taskId, message));
		InetSocketAddress scheduler = requestSchedulers.get(taskId.requestId);
		if (scheduler == null) {
			LOG.error("Did not find any scheduler info for request: " + taskId);
			return;
		}

		try {
			AsyncClient client = schedulerClientPool.borrowClient(scheduler);
			client.sendFrontendMessage(app, taskId, status, message, new sendFrontendMessageCallback(scheduler, client));
			LOG.debug("finished sending message");
		} catch (IOException e) {
			LOG.error(e);
		} catch (TException e) {
			LOG.error(e);
		} catch (Exception e) {
			LOG.error(e);
		}
	}

	public Boolean getStealing() {
		return stealing;
	}

	// Only for testing
	public void setStealing(Boolean stealing) {
		this.stealing = stealing;
	}

}
