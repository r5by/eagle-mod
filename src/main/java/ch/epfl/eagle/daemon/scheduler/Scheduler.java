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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ch.epfl.eagle.daemon.EagleConf;
import ch.epfl.eagle.daemon.util.Logging;
import ch.epfl.eagle.daemon.util.Network;
import ch.epfl.eagle.daemon.util.Serialization;
import ch.epfl.eagle.daemon.util.ThriftClientPool;
import ch.epfl.eagle.thrift.FrontendService;
import ch.epfl.eagle.thrift.FrontendService.AsyncClient.frontendMessage_call;
import ch.epfl.eagle.thrift.InternalService;
import ch.epfl.eagle.thrift.InternalService.AsyncClient;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.enqueueTaskReservations_call;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.enqueueTasksCentralized_call;
import ch.epfl.eagle.thrift.InternalService.AsyncClient.probeEnqueueTaskReservations_call;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsRequest;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsResponse;
import ch.epfl.eagle.thrift.TFullTaskId;
import ch.epfl.eagle.thrift.THostPort;
import ch.epfl.eagle.thrift.TPlacementPreference;
import ch.epfl.eagle.thrift.TSchedulingRequest;
import ch.epfl.eagle.thrift.TTaskLaunchSpec;
import ch.epfl.eagle.thrift.TTaskSpec;

/**
 * This class implements the Eagle scheduler functionality.
 */
public class Scheduler {
	private final static Logger LOG = Logger.getLogger(Scheduler.class);
	private final static Logger AUDIT_LOG = Logging.getAuditLogger(Scheduler.class);

	/** Used to uniquely identify requests arriving at this scheduler. */
	// private AtomicInteger counter = new AtomicInteger(new
	// java.util.Random().nextInt(30000));
	private AtomicInteger counter = new AtomicInteger(0);

	/**
	 * Used to keep the reservation records of each arrived request; Initialized at handleJobSubmission; key->value is requestId -> defined book-keeping structure
	 */
	HashMap<String, RequestTasksRecords> records = new HashMap<String, RequestTasksRecords>();

	/** How many times the special case has been triggered. */
	private AtomicInteger specialCaseCounter = new AtomicInteger(0);

	private THostPort address;

	/** Socket addresses for each frontend. */
	HashMap<String, InetSocketAddress> frontendSockets = new HashMap<String, InetSocketAddress>();

	/**
	 * Service that handles cancelling outstanding reservations for jobs that
	 * have already been scheduled. Only instantiated if
	 * {@code EagleConf.CANCELLATION} is set to true.
	 */
	private CancellationService cancellationService;
	private boolean useCancellation;
	// [[FLORIN
	private String hostnameCentralizedScheduler;
	public boolean amIMaster = false;
	public double smallPartition = 0;
	public double bigPartition = 0;
	
	/** EAGLE PIGGYBACKING */
	// Centralized scheduler
	public boolean piggybacking = false;
	// Distributed scheduler
	private List<String> distributedNotExecutingLong;
	private double distributedLongStatusTimestamp;
	private HashMap<String, Integer> outstandingCallbackRequests = new HashMap<String, Integer>();
	private HashMap<String, List<InetSocketAddress>> requestsToRelocate = new HashMap<String, List<InetSocketAddress>>();
	private int retry_rounds;
	private HashMap<String, Integer> outstandingRequestsRoundsLeft = new HashMap<String, Integer>();
	public boolean last_round_short_partition = true;
	 
	/** Thrift client pool for communicating with node monitors */
	ThriftClientPool<InternalService.AsyncClient> nodeMonitorClientPool = new ThriftClientPool<InternalService.AsyncClient>(
			new ThriftClientPool.InternalServiceMakerFactory());

	/** Thrift client pool for communicating with front ends. */
	private ThriftClientPool<FrontendService.AsyncClient> frontendClientPool = new ThriftClientPool<FrontendService.AsyncClient>(
			new ThriftClientPool.FrontendServiceMakerFactory());

	/** Information about cluster workload due to other schedulers. */
	private SchedulerState state;

	/**
	 * Probe ratios to use if the probe ratio is not explicitly set in the
	 * request.
	 */
	private double defaultProbeRatioUnconstrained;
	private double defaultProbeRatioConstrained;

	/**
	 * For each request, the task placer that should be used to place the
	 * request's tasks. Indexed by the request ID.
	 */
	private ConcurrentMap<String, TaskPlacer> requestTaskPlacers;

	/**
	 * When a job includes SPREAD_EVENLY in the description and has this number
	 * of tasks, Eagle spreads the tasks evenly over machines to evenly cache
	 * data. We need this (in addition to the SPREAD_EVENLY descriptor) because
	 * only the reduce phase -- not the map phase -- should be spread.
	 */
	private int spreadEvenlyTaskSetSize;

	private Configuration conf;

	public void initialize(Configuration conf, InetSocketAddress socket) throws IOException {
		address = Network.socketAddressToThrift(socket);
		String mode = conf.getString(EagleConf.DEPLOYMENT_MODE, "unspecified");
		this.conf = conf;
		if (mode.equals("standalone")) {
			state = new StandaloneSchedulerState();
		} else if (mode.equals("configbased")) {
			state = new ConfigSchedulerState();
		} else {
			throw new RuntimeException("Unsupported deployment mode: " + mode);
		}

		state.initialize(conf);

		defaultProbeRatioUnconstrained = conf.getDouble(EagleConf.SAMPLE_RATIO, EagleConf.DEFAULT_SAMPLE_RATIO);
		defaultProbeRatioConstrained = conf.getDouble(EagleConf.SAMPLE_RATIO_CONSTRAINED, EagleConf.DEFAULT_SAMPLE_RATIO_CONSTRAINED);

		requestTaskPlacers = Maps.newConcurrentMap();

		useCancellation = conf.getBoolean(EagleConf.CANCELLATION, EagleConf.DEFAULT_CANCELLATION);

		// [[FLORIN
		this.hostnameCentralizedScheduler = conf.getString("scheduler.centralized", "none");
		this.smallPartition = conf.getInt(EagleConf.SMALL_PARTITION, EagleConf.DEFAULT_SMALL_PARTITION);
		this.bigPartition = conf.getInt(EagleConf.BIG_PARTITION, EagleConf.DEFAULT_BIG_PARTITION);

		// if(address.getHost().contains(this.hostnameCentralizedScheduler)){
		if (address.getHost().matches(this.hostnameCentralizedScheduler)) {
			amIMaster = true;
			LOG.info("I am master: " + this.hostnameCentralizedScheduler + "--" + address.getHost() + "--");
		}
		// LOG.info("I am NOT master: "+this.hostnameCentralizedScheduler+"--"+address.getHost()+"--");

		// EAGLE
		this.piggybacking = conf.getBoolean(EagleConf.PIGGYBACKING, EagleConf.DEFAULT_PIGGYBACKING);
		LOG.info("Piggybacking: " + this.piggybacking);
		distributedLongStatusTimestamp = -1;
		distributedNotExecutingLong = new ArrayList<String>();
		this.retry_rounds = conf.getInt(EagleConf.RETRY_ROUNDS, EagleConf.DEFAULT_RETRY_ROUNDS);
		LOG.info("retry_rounds: " + this.retry_rounds);
		this.last_round_short_partition = conf.getBoolean(EagleConf.LAST_ROUND_SHORT_PARTITION, EagleConf.DEFAULT_LAST_ROUND_SHORT_PARTITION);
		
		if (useCancellation) {
			LOG.debug("Initializing cancellation service");
			cancellationService = new CancellationService(nodeMonitorClientPool);
			new Thread(cancellationService).start();
		} else {
			LOG.debug("Not using cancellation");
		}

		spreadEvenlyTaskSetSize = conf.getInt(EagleConf.SPREAD_EVENLY_TASK_SET_SIZE, EagleConf.DEFAULT_SPREAD_EVENLY_TASK_SET_SIZE);
	}

	public boolean registerFrontend(String appId, String addr) {
		LOG.debug(Logging.functionCall(appId, addr));
		Optional<InetSocketAddress> socketAddress = Serialization.strToSocket(addr);
		if (!socketAddress.isPresent()) {
			LOG.error("Bad address from frontend: " + addr);
			return false;
		}
		frontendSockets.put(appId, socketAddress.get());
		return state.watchApplication(appId);
	}

	/**
	 * Callback for enqueueTaskReservations() that returns the client to the
	 * client pool.
	 */
	private class EnqueueTaskReservationsCallback implements AsyncMethodCallback<enqueueTaskReservations_call> {
		String requestId;
		InetSocketAddress nodeMonitorAddress;
		long startTimeMillis;

		public EnqueueTaskReservationsCallback(String requestId, InetSocketAddress nodeMonitorAddress) {
			this.requestId = requestId;
			this.nodeMonitorAddress = nodeMonitorAddress;
			this.startTimeMillis = System.currentTimeMillis();
		}

		public void onComplete(enqueueTaskReservations_call response) {
			AUDIT_LOG.debug(Logging.auditEventString("scheduler_complete_enqueue_task", requestId, nodeMonitorAddress.getAddress().getHostAddress()));
			long totalTime = System.currentTimeMillis() - startTimeMillis;
			LOG.debug("Enqueue Task RPC to " + nodeMonitorAddress.getAddress().getHostAddress() + " for request " + requestId + " completed in " + totalTime
					+ "ms");
			try {
				nodeMonitorClientPool.returnClient(nodeMonitorAddress, (AsyncClient) response.getClient());
			} catch (Exception e) {
				LOG.error("Error returning client to node monitor client pool: " + e);
			}
			return;
		}

		public void onError(Exception exception) {
			// Do not return error client to pool
			LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
		}
	}
	
   // ************************************************************
	// EAGLE Adding special EnqueueTaskReservationsCallback for centralized and modifying 
   // ************************************************************
	// Exactly as the older version enqueueTaskReservations(), only different class names
	private class EnqueueTasksCentralizedCallback implements AsyncMethodCallback<enqueueTasksCentralized_call> {
		String requestId;
		InetSocketAddress nodeMonitorAddress;
		long startTimeMillis;

		public EnqueueTasksCentralizedCallback(String requestId, InetSocketAddress nodeMonitorAddress) {
			this.requestId = requestId;
			this.nodeMonitorAddress = nodeMonitorAddress;
			this.startTimeMillis = System.currentTimeMillis();
		}

		@Override
		public void onComplete(enqueueTasksCentralized_call response) {
			AUDIT_LOG.debug(Logging.auditEventString("scheduler_complete_enqueue_task", requestId, nodeMonitorAddress.getAddress().getHostAddress()));
			long totalTime = System.currentTimeMillis() - startTimeMillis;
			LOG.debug("Enqueue Task RPC to " + nodeMonitorAddress.getAddress().getHostAddress() + " for request " + requestId + " completed in " + totalTime
					+ "ms");
			try {
				nodeMonitorClientPool.returnClient(nodeMonitorAddress, (AsyncClient) response.getClient());
			} catch (Exception e) {
				LOG.error("Error returning client to node monitor client pool: " + e);
			}
			return;
		}

		public void onError(Exception exception) {
			// Do not return error client to pool
			LOG.error("Error executing EnqueueTasksCentralizedCallback RPC:" + exception);
		}
	}

	private class ProbeEnqueueTaskReservationsCallback implements AsyncMethodCallback<probeEnqueueTaskReservations_call> {
		String requestId;
		InetSocketAddress nodeMonitorAddress;
		long startTimeMillis;

		public ProbeEnqueueTaskReservationsCallback(String requestId, InetSocketAddress nodeMonitorAddress) {
			this.requestId = requestId;
			this.nodeMonitorAddress = nodeMonitorAddress;
			this.startTimeMillis = System.currentTimeMillis();
		}

		public void onComplete(probeEnqueueTaskReservations_call response) {
			AUDIT_LOG.debug(Logging.auditEventString("scheduler_complete_enqueue_task", requestId, nodeMonitorAddress.getAddress().getHostAddress()));
			long totalTime = System.currentTimeMillis() - startTimeMillis;
			LOG.debug("EAGLE Distributed scheduler. Enqueue Task RPC to " + nodeMonitorAddress.getAddress().getHostAddress() + " for request " + requestId + " completed in " + totalTime
					+ "ms");
			try {
				nodeMonitorClientPool.returnClient(nodeMonitorAddress, (AsyncClient) response.getClient());
				TEnqueueTaskReservationsResponse responseValue = response.getResult();	
				LOG.debug("EAGLE Distributed scheduler callback. Response values, timestamp: "+ responseValue.getLongStatusTimestamp()+" long in queue or executing: "+ responseValue.isLongInQueue()+ " notExecutingLong "+responseValue.getNotExecutingLong().size());
				// Update longStatus if needed
				if (distributedLongStatusTimestamp<responseValue.getLongStatusTimestamp()){
					distributedLongStatusTimestamp = responseValue.getLongStatusTimestamp();
					distributedNotExecutingLong = responseValue.getNotExecutingLong();
				}					
				// Wait to get answer from all and then send back the missing probes/tasks
				int  waitingToAnswer = outstandingCallbackRequests.get(requestId);
				waitingToAnswer--;
				if(responseValue.isLongInQueue()){
					List<InetSocketAddress> listToRelocate = requestsToRelocate.get(requestId);
					listToRelocate.add(nodeMonitorAddress);
					requestsToRelocate.put(requestId, listToRelocate);
				}
				LOG.debug("EAGLE Distributed scheduler callback. NEW VALUES: timestamp "+ distributedLongStatusTimestamp+" notExecutingLong "+ distributedNotExecutingLong.size()+ " waitingToAnswer "+waitingToAnswer+ " to relocate "+ requestsToRelocate.get(requestId).size());
				if(waitingToAnswer==0){
					LOG.debug("EAGLE waitingToAnswer 0, going to relocate " + requestsToRelocate.get(requestId).size());
					outstandingCallbackRequests.remove(requestId);
					if (requestsToRelocate.get(requestId).size()>0)
						relocateMissingProbes(requestId, requestsToRelocate.get(requestId));
					else
						requestsToRelocate.remove(requestId);
				}
				else
					outstandingCallbackRequests.put(requestId, waitingToAnswer);
			} catch (Exception e) {
				LOG.error("Error returning client to node monitor client pool: " + e);
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				sw.toString();
				LOG.error("Error stacktrace "+sw);
			}
			return;
		}

		public void onError(Exception exception) {
			// Do not return error client to pool
			LOG.error("Error executing enqueueTaskReservation RPC:" + exception);
		}
	}

	// ************************************************************
   // ************************************************************
	
	/**
	 * Adds constraints such that tasks in the job will be spread evenly across
	 * the cluster.
	 * 
	 * We expect three of these special jobs to be submitted; 3 sequential calls
	 * to this method will result in spreading the tasks for the 3 jobs across
	 * the cluster such that no more than 1 task is assigned to each machine.
	 */
	private TSchedulingRequest addConstraintsToSpreadTasks(TSchedulingRequest req) throws TException {
		LOG.info("Handling spread tasks request: " + req);
		int specialCaseIndex = specialCaseCounter.incrementAndGet();
		if (specialCaseIndex < 1 || specialCaseIndex > 3) {
			LOG.error("Invalid special case index: " + specialCaseIndex);
		}

		// No tasks have preferences and we have the magic number of tasks
		TSchedulingRequest newReq = new TSchedulingRequest();
		newReq.user = req.user;
		newReq.app = req.app;
		newReq.probeRatio = req.probeRatio;

		List<InetSocketAddress> allBackends = Lists.newArrayList();
		List<InetSocketAddress> backends = Lists.newArrayList();
		// We assume the below always returns the same order (invalid
		// assumption?)
		for (InetSocketAddress backend : state.getBackends(req.app)) {
			allBackends.add(backend);
		}

		// Each time this is called, we restrict to 1/3 of the nodes in the
		// cluster
		for (int i = 0; i < allBackends.size(); i++) {
			if (i % 3 == specialCaseIndex - 1) {
				backends.add(allBackends.get(i));
			}
		}
		Collections.shuffle(backends);

		if (!(allBackends.size() >= (req.getTasks().size() * 3))) {
			LOG.error("Special case expects at least three times as many machines as tasks.");
			return null;
		}
		LOG.info(backends);
		for (int i = 0; i < req.getTasksSize(); i++) {
			TTaskSpec task = req.getTasks().get(i);
			TTaskSpec newTask = new TTaskSpec();
			newTask.message = task.message;
			newTask.taskId = task.taskId;
			newTask.preference = new TPlacementPreference();
			newTask.preference.addToNodes(backends.get(i).getHostName());
			newReq.addToTasks(newTask);
		}
		LOG.info("New request: " + newReq);
		return newReq;
	}

	/**
	 * Checks whether we should add constraints to this job to evenly spread
	 * tasks over machines.
	 * 
	 * This is a hack used to force Spark to cache data in 3 locations: we run 3
	 * select * queries on the same table and spread the tasks for those queries
	 * evenly across the cluster such that the input data for the query is triple
	 * replicated and spread evenly across the cluster.
	 * 
	 * We signal that Eagle should use this hack by adding SPREAD_TASKS to the
	 * job's description.
	 */
	private boolean isSpreadTasksJob(TSchedulingRequest request) {
		if ((request.getDescription() != null) && (request.getDescription().indexOf("SPREAD_EVENLY") != -1)) {
			// Need to check to see if there are 3 constraints; if so, it's the
			// map phase of the
			// first job that reads the data from HDFS, so we shouldn't override
			// the constraints.
			for (TTaskSpec t : request.getTasks()) {
				if (t.getPreference() != null && (t.getPreference().getNodes() != null) && (t.getPreference().getNodes().size() == 3)) {
					LOG.debug("Not special case: one of request's tasks had 3 preferences");
					return false;
				}
			}
			if (request.getTasks().size() != spreadEvenlyTaskSetSize) {
				LOG.debug("Not special case: job had " + request.getTasks().size() + " tasks rather than the expected " + spreadEvenlyTaskSetSize);
				return false;
			}
			if (specialCaseCounter.get() >= 3) {
				LOG.error("Not using special case because special case code has already been " + " called 3 more more times!");
				return false;
			}
			LOG.debug("Spreading tasks for job with " + request.getTasks().size() + " tasks");
			return true;
		}
		LOG.debug("Not special case: description did not contain SPREAD_EVENLY");
		return false;
	}

	public void submitJob(TSchedulingRequest request) throws TException {
		// Short-circuit case that is used for liveness checking
		if (request.tasks.size() == 0) {
			return;
		}
		if (isSpreadTasksJob(request)) {
			handleJobSubmission(addConstraintsToSpreadTasks(request));
		} else {
			handleJobSubmission(request);
		}
	}

	public void handleJobSubmission(TSchedulingRequest request) throws TException {
		LOG.debug(Logging.functionCall(request));
		LOG.debug("Handling job submission, shortJob " + request.shortJob + " estimated duration " + request.estimatedDuration);
		if (amIMaster && conf.getBoolean(EagleConf.USE_GIVEN_DURATIONS, EagleConf.DEFAULT_USE_GIVEN_DURATIONS)) {
			request.shortJob = false;
			request.estimatedDuration = conf.getDouble(EagleConf.GIVEN_BIG_DURATIONS, EagleConf.DEFAULT_GIVEN_BIG_DURATIONS);
			LOG.info("CENTRALIZED: Using given durations for long jobs!!! Going to overwrite those given from frontend, new given duration "
					+ request.estimatedDuration);
		}

		long start = System.currentTimeMillis();

		String requestId = getRequestId();

		//Init records for the request
		LOG.debug("Eagle start handling request: " + requestId + " at time stamp: " + start);
		records.put(requestId, new RequestTasksRecords(start, request.getTasksSize(), request.shortJob));

		String user = "";
		if (request.getUser() != null && request.getUser().getUser() != null) {
			user = request.getUser().getUser();
		}
		String description = "";
		if (request.getDescription() != null) {
			description = request.getDescription();
		}

		String app = request.getApp();
		List<TTaskSpec> tasks = request.getTasks();
		Set<InetSocketAddress> backends = state.getBackends(app);
		LOG.debug("NumBackends: " + backends.size() + " tasks size " + tasks.size());
		boolean constrained = false;
		for (TTaskSpec task : tasks) {
			task.preference = null;
			constrained = constrained || (task.preference != null && task.preference.nodes != null && !task.preference.nodes.isEmpty());
		}
		assert (!constrained);
		// Logging the address here is somewhat redundant, since all of the
		// messages in this particular log file come from the same address.
		// However, it simplifies the process of aggregating the logs, and will
		// also be useful when we support multiple daemons running on a single
		// machine.
		AUDIT_LOG.info(Logging.auditEventString("arrived", requestId, request.getTasks().size(), address.getHost(), address.getPort(), user, description,
				constrained));

		// [[FLORIN
		TaskPlacer taskPlacer;
		double requestGetProbeRatio = -1;
		if (request.isSetProbeRatio())
			requestGetProbeRatio = request.getProbeRatio();
		if (amIMaster) {
			LOG.debug("Centralized scheduler setting requestGetProbeRatio to 1");
			requestGetProbeRatio = 1;
			defaultProbeRatioConstrained = 1;
			defaultProbeRatioUnconstrained = 1;
		}

		LOG.debug("Handling job submision, constrained?? " + constrained + " am I centralized? " + amIMaster);
		/*
		 * if (constrained && true) { // Not only for centralized but for all:
		 * will always be unconstrained if (request.isSetProbeRatio()) {
		 * taskPlacer = new ConstrainedTaskPlacer(requestId,
		 * requestGetProbeRatio); } else { taskPlacer = new
		 * ConstrainedTaskPlacer(requestId, defaultProbeRatioConstrained); } }
		 * else {(
		 */
		if (request.isSetProbeRatio()) {
			taskPlacer = new UnconstrainedTaskPlacer(requestId, requestGetProbeRatio, this);
		} else {
			taskPlacer = new UnconstrainedTaskPlacer(requestId, defaultProbeRatioUnconstrained, this);
		}
		// }
		assert (taskPlacer instanceof UnconstrainedTaskPlacer);
		requestTaskPlacers.put(requestId, taskPlacer);

		Map<InetSocketAddress, TEnqueueTaskReservationsRequest> enqueueTaskReservationsRequests;
		enqueueTaskReservationsRequests = taskPlacer.getEnqueueTaskReservationsRequests_wrapper(request, requestId, backends, address);

		List<String> notExecutingLong = null;
		double longStatusTimestamp = -1;
		
		if(amIMaster && piggybacking) {
			notExecutingLong = taskPlacer.getNotExecutingLong(backends);
			longStatusTimestamp = System.currentTimeMillis();
			LOG.debug("EAGLE CENTRALIZED NOTEXECUTINGLONG requestId " + requestId + " on nodes: " + enqueueTaskReservationsRequests.size() + " notExecutingLong: " + notExecutingLong + " total nodes "+backends.size());
		} 
		else if(piggybacking){
			outstandingRequestsRoundsLeft.put(requestId, this.retry_rounds);
			int pendingRequests = enqueueTaskReservationsRequests.entrySet().size();
			LOG.debug(" pendingRequests "+pendingRequests);
			outstandingCallbackRequests.put(requestId, pendingRequests); // TODO FIX wont work if more than one probe per worker
			requestsToRelocate.put(requestId, new ArrayList<InetSocketAddress>());
		}
		
		// Request to enqueue a task at each of the selected nodes.
		for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : enqueueTaskReservationsRequests.entrySet()) {
			try {
				InternalService.AsyncClient client = nodeMonitorClientPool.borrowClient(entry.getKey());
				LOG.debug("Launching enqueueTask for request " + requestId + "on node: " + entry.getKey());
				AUDIT_LOG
						.debug(Logging.auditEventString("scheduler_launch_enqueue_task", entry.getValue().requestId, entry.getKey().getAddress().getHostAddress()));
				if(amIMaster && piggybacking)
					client.enqueueTasksCentralized(entry.getValue(), notExecutingLong, longStatusTimestamp, new EnqueueTasksCentralizedCallback(requestId, entry.getKey()));					
				else if(piggybacking)
					client.probeEnqueueTaskReservations(entry.getValue(), new ProbeEnqueueTaskReservationsCallback(requestId, entry.getKey()));
				else					
					client.enqueueTaskReservations(entry.getValue(), new EnqueueTaskReservationsCallback(requestId, entry.getKey()));
			} catch (Exception e) {
				LOG.error("Error enqueuing task on node " + entry.getKey().toString() + ":" + e);
			}
		}
		long end = System.currentTimeMillis();
		LOG.debug("All tasks enqueued for request " + requestId + "; returning. Total time: " + (end - start) + " milliseconds");		
	}

	public Set<InetSocketAddress> getAllBackends(String app){
		return state.getBackends(app);
   }
	
	public void relocateMissingProbes(String requestId, List<InetSocketAddress> probestoRelocate) {
		TaskPlacer taskPlacer = requestTaskPlacers.get(requestId);
		Map<InetSocketAddress, TEnqueueTaskReservationsRequest> enqueueTaskReservationsRequests;
		if(taskPlacer==null)
			return;
		LOG.debug("RELOCATING request "+ requestId+ " rounds left "+outstandingRequestsRoundsLeft.get(requestId)+" getschedulingrequest "+ taskPlacer.getSchedulingRequest());
		String app = taskPlacer.getSchedulingRequest().getApp();
		Set<InetSocketAddress> allBackends = state.getBackends(app);

		// Put the ones in the list only
		Set<InetSocketAddress> backends = new HashSet<InetSocketAddress>();
		for (InetSocketAddress backend : allBackends){
			if(distributedNotExecutingLong.contains(backend.toString())){
				backends.add(backend);
			}
		}
		int roundsLeft = outstandingRequestsRoundsLeft.get(requestId);
		roundsLeft--;
		assert(roundsLeft>=0);
		LOG.debug("relocateMissingProbes distributedNotExecutingLong.size() " + distributedNotExecutingLong.size() + " free backends " + backends.size());
		
		enqueueTaskReservationsRequests = taskPlacer.getRelocateEnqueueTaskRequests(requestId, probestoRelocate, backends, address, roundsLeft==0);
		long start = System.currentTimeMillis();
		
		int pendingRequests = enqueueTaskReservationsRequests.entrySet().size();
		LOG.debug("relocateMissingProbes pendingRequests "+pendingRequests);
		outstandingCallbackRequests.put(requestId, pendingRequests);
		//requestsToRelocate.remove(requestId);
		requestsToRelocate.put(requestId, new ArrayList<InetSocketAddress>());
		
		
		// Request to enqueue a task at each of the selected nodes.
		for (Entry<InetSocketAddress, TEnqueueTaskReservationsRequest> entry : enqueueTaskReservationsRequests.entrySet()) {
			try {
				InternalService.AsyncClient client = nodeMonitorClientPool.borrowClient(entry.getKey());
				LOG.debug("RELaunching enqueueTask for request " + requestId + "on node: " + entry.getKey()+" rounds left "+ roundsLeft);
				if(roundsLeft==0) // End normally as before
					client.enqueueTaskReservations(entry.getValue(), new EnqueueTaskReservationsCallback(requestId, entry.getKey()));
				else{
					client.probeEnqueueTaskReservations(entry.getValue(), new ProbeEnqueueTaskReservationsCallback(requestId, entry.getKey()));
					outstandingRequestsRoundsLeft.put(requestId,roundsLeft);
				}
			} catch (Exception e) {
				LOG.error("Error enqueuing task on node " + entry.getKey().toString() + ":" + e);
			}
		}
		
		long end = System.currentTimeMillis();
		LOG.debug("All tasks rescheduled for request " + requestId + "; returning. Total time: " + (end - start) + " milliseconds");		
	}
	
	public List<TTaskLaunchSpec> getTask(String requestId, THostPort nodeMonitorAddress, THostPort oldNodeMonitorAddress) {
		/*
		 * TODO: Consider making this synchronized to avoid the need for
		 * synchronization in the task placers (although then we'd lose the
		 * ability to parallelize over task placers).
		 */
		LOG.debug(Logging.functionCall(requestId, nodeMonitorAddress));
		TaskPlacer taskPlacer = requestTaskPlacers.get(requestId);
		if (taskPlacer == null) {
			LOG.debug("Received getTask() request for request " + requestId + ", which had no more " + "unplaced tasks");
			return Lists.newArrayList();
		}

		synchronized (taskPlacer) {
			List<TTaskLaunchSpec> taskLaunchSpecs = taskPlacer.assignTask(nodeMonitorAddress, oldNodeMonitorAddress);
			if (taskLaunchSpecs == null || taskLaunchSpecs.size() > 1) {
				LOG.error("Received invalid task placement for request " + requestId + ": " + taskLaunchSpecs.toString());
				return Lists.newArrayList();
			} else if (taskLaunchSpecs.size() == 1) {
				AUDIT_LOG.info(Logging.auditEventString("scheduler_assigned_task", requestId, taskLaunchSpecs.get(0).taskId, nodeMonitorAddress.getHost()));
			} else {
				AUDIT_LOG.info(Logging.auditEventString("scheduler_get_task_no_task", requestId, nodeMonitorAddress.getHost()));
			}

			if (taskPlacer.allTasksPlaced()) {
				LOG.debug("All tasks placed for request " + requestId);
				requestTaskPlacers.remove(requestId);
				if (useCancellation) {
					Set<THostPort> outstandingNodeMonitors = taskPlacer.getOutstandingNodeMonitorsForCancellation();
					for (THostPort nodeMonitorToCancel : outstandingNodeMonitors) {
						cancellationService.addCancellation(requestId, nodeMonitorToCancel);
					}
				}
			}
			return taskLaunchSpecs;
		}
	}

	/**
	 * Returns an ID that identifies a request uniquely (across all Eagle
	 * schedulers).
	 * 
	 * This should only be called once for each request (it will return a
	 * different identifier if called a second time).
	 * 
	 * TODO: Include the port number, so this works when there are multiple
	 * schedulers running on a single machine.
	 */
	private String getRequestId() {
		/*
		 * The request id is a string that includes the IP address of this
		 * scheduler followed by the counter. We use a counter rather than a hash
		 * of the request because there may be multiple requests to run an
		 * identical job.
		 */
		return String.format("%s_%d", Network.getIPAddress(conf), counter.getAndIncrement());
	}

	private class sendFrontendMessageCallback implements AsyncMethodCallback<frontendMessage_call> {
		private InetSocketAddress frontendSocket;
		private FrontendService.AsyncClient client;

		public sendFrontendMessageCallback(InetSocketAddress socket, FrontendService.AsyncClient client) {
			frontendSocket = socket;
			this.client = client;
		}

		public void onComplete(frontendMessage_call response) {
			try {
				frontendClientPool.returnClient(frontendSocket, client);
			} catch (Exception e) {
				LOG.error(e);
			}
		}

		public void onError(Exception exception) {
			// Do not return error client to pool
			LOG.error("Error sending frontend message callback: " + exception);
		}
	}

	public void sendFrontendMessage(String app, TFullTaskId taskId, int status, ByteBuffer message) {
		LOG.debug(Logging.functionCall(app, taskId, message));
		InetSocketAddress frontend = frontendSockets.get(app);

		//When a task is completed, remove the task from the request
		String requestId = taskId.requestId;
		RequestTasksRecords record = records.get(requestId);
		record.handleTaskComplete();

		if (frontend == null) {
			LOG.error("Requested message sent to unregistered app: " + app);
		}
		try {
//			FrontendService.AsyncClient client = frontendClientPool.borrowClient(frontend);
//			client.frontendMessage(taskId, status, message, new sendFrontendMessageCallback(frontend, client));
			FrontendService.AsyncClient client = frontendClientPool.borrowClient(frontend);

			synchronized (record) {
				if(record.allTasksCompleted()) { //If all tasks of the request completed, set status as 1 and pass the elapsed time to frontend output
					ByteBuffer msg = ByteBuffer.allocate(8);
					msg.putLong(record.elapsed());
					msg.position(0);

					/* The updated status:
					* 0: Original value, sent back to client if not all tasks completed
					* 1: All tasks completed for the request, and the request is short job
					* 2: All tasks completed for the request, and the request is long job
					* */
					int updatedStatus = status;

					if(record.isShortjob())
						updatedStatus = 1;
					else
						updatedStatus = 2;

					client.frontendMessage(taskId, updatedStatus, msg,
							new sendFrontendMessageCallback(frontend, client));

					records.remove(requestId);
				} else {
					client.frontendMessage(taskId, status, message,
							new sendFrontendMessageCallback(frontend, client));
				}
			}

		} catch (IOException e) {
			LOG.error("Error launching message on frontend: " + app, e);
		} catch (TException e) {
			LOG.error("Error launching message on frontend: " + app, e);
		} catch (Exception e) {
			LOG.error("Error launching message on frontend: " + app, e);
		}
	}

	//===============================
	// Extension: Instruments
	//===============================

	/**
	 * Per-request related reservation book-keeping. Used to instrument Sparrow so we know the elapsed time for every request (the time starting from incoming request gets scheduled, to the last of its task finished)
	 */
	class RequestTasksRecords {
		long start;
		long end;
		int remainingTasks;
		boolean shortjob;

		RequestTasksRecords(long pStart, int tasksSize, boolean isShortjob) {
			start = pStart;
			remainingTasks = tasksSize;
			shortjob = isShortjob;
		}

		/* When a task complete, decrease the reservations; if no reservations left record the end time stamp */
		void handleTaskComplete() {
			remainingTasks--;
			if(remainingTasks ==0)
				end = System.currentTimeMillis();
		}

		boolean allTasksCompleted() {
			if(remainingTasks == 0) {
				remainingTasks = -1; //Reset the flag to avoid multiple access the critical section
				return true;
			} else
				return false;
		}

		long elapsed() {
			return end - start;
		}

		boolean isShortjob() {
			return shortjob;
		}
	}
}
