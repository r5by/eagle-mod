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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Iterator;

import org.apache.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ch.epfl.eagle.daemon.util.Logging;
import ch.epfl.eagle.thrift.TEnqueueTaskReservationsRequest;
import ch.epfl.eagle.thrift.THostPort;
import ch.epfl.eagle.thrift.TSchedulingRequest;
import ch.epfl.eagle.thrift.TTaskLaunchSpec;
import ch.epfl.eagle.thrift.TTaskSpec;

/**
 * A task placer for jobs whose tasks have no placement constraints.
 */
public class UnconstrainedTaskPlacer implements TaskPlacer {
	private static final Logger LOG = Logger.getLogger(UnconstrainedTaskPlacer.class);

	/** Specifications for tasks that have not yet been launched. */
	List<TTaskLaunchSpec> unlaunchedTasks;

	public class NodeWaitTuple implements Comparable {
		public InetSocketAddress node;
		public Double wait;

		public NodeWaitTuple(InetSocketAddress node, double wait) {
			this.node = node;
			this.wait = wait;
		}

		public int compareTo(Object t2) {
			return this.wait.compareTo(((NodeWaitTuple) t2).wait);
		}
	}

	/**
	 * For each node monitor where reservations were enqueued, the number of
	 * reservations that were enqueued there.
	 */
	private Map<THostPort, Integer> outstandingReservations;
	// private static Map<THostPort, ArrayList<Integer> >
	// outstandingReservationsEstimatedDuration;
	private static Map<THostPort, Double> outstandingReservationsTotalWaitTime = new HashMap<THostPort, Double>();
	private static Map<THostPort, Long> startTstampsCurrentlyExecBigJobs = new HashMap<THostPort, Long>();
	private static Map<THostPort, Double> estimateCurrentlyExecBigJobs = new HashMap<THostPort, Double>();

	// [[FLORIN
	private Map<String, Long> startTimestampOfCurrentlyExecutingTask;
	private double estimatedDuration;
	private TSchedulingRequest schedulingRequest;
	/** Whether the remaining reservations have been cancelled. */
	boolean cancelled;

	/**
	 * Id of the request associated with this task placer.
	 */
	String requestId;
	Scheduler scheduler;

	private double probeRatio;

	UnconstrainedTaskPlacer(String requestId, double probeRatio, Scheduler scheduler) {
		this.requestId = requestId;
		this.probeRatio = probeRatio;
		unlaunchedTasks = new LinkedList<TTaskLaunchSpec>();
		outstandingReservations = new HashMap<THostPort, Integer>();
		// outstandingReservationsEstimatedDuration = new HashMap<THostPort,
		// ArrayList<Integer>>();
		// UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime = new
		// HashMap<THostPort, Integer>();
		startTimestampOfCurrentlyExecutingTask = new HashMap<String, Long>();
		cancelled = false;
		// [[FLORIN
		LOG.info("Creating UnconstrainedTaskPlacer for requestId: " + requestId + " probeRatio: " + probeRatio);
		this.scheduler = scheduler;
	}

	public Map<InetSocketAddress, TEnqueueTaskReservationsRequest> getEnqueueTaskReservationsRequests_wrapper(TSchedulingRequest schedulingRequest,
			String requestId, Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {

		if (scheduler.amIMaster == true)
			return getEnqueueTaskReservationsRequests_Centralized(schedulingRequest, requestId, nodes, schedulerAddress);
		else
			return getEnqueueTaskReservationsRequests(schedulingRequest, requestId, nodes, schedulerAddress);
	}

	public Map<InetSocketAddress, TEnqueueTaskReservationsRequest> getEnqueueTaskReservationsRequests_Centralized(TSchedulingRequest schedulingRequest,
			String requestId, Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {

		this.estimatedDuration = schedulingRequest.estimatedDuration;

		int numTasks = schedulingRequest.getTasks().size();
		int reservationsToLaunch = (int) Math.ceil(probeRatio * numTasks);
		LOG.info("getEnqueueTaskReservationsRequests_Centralized - Request " + requestId + ": Creating " + reservationsToLaunch + " task reservations for "
				+ numTasks + " tasks. Estimated duration: " + estimatedDuration);

		// Get the big partition
		List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
		Collections.sort(nodeList, new Comparator<InetSocketAddress>() {
			public int compare(InetSocketAddress a1, InetSocketAddress a2) {
				return a1.toString().compareTo(a2.toString());
			}
		});

		LOG.info("Initial node list size: " + nodeList.size());
		int last_nodeID = (int) (nodeList.size() * scheduler.bigPartition / 100);
		nodeList = nodeList.subList(0, last_nodeID);
		LOG.info("Using nodes from 0 to " + last_nodeID + ". List consists of : " + nodeList);

		for (TTaskSpec task : schedulingRequest.getTasks()) {
			TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(), task.bufferForMessage());
			unlaunchedTasks.add(taskLaunchSpec);
		}

		// hashmap to return from method
		HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();
		HashMap<InetSocketAddress, Integer> resPerNode = Maps.newHashMap();

		PriorityQueue<NodeWaitTuple> pq = new PriorityQueue<NodeWaitTuple>(10);

		synchronized (UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime) {
			for (InetSocketAddress isa : nodeList) {
				THostPort t_node = new THostPort(isa.getAddress().getHostAddress(), isa.getPort());
				double wait = 0;
				if (UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.containsKey(t_node)) {
					wait = UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(t_node);
					LOG.info("When build pq found node " + t_node + " in outstandingReservationsTotalWaitTime with wait time: " + wait);
				}

				// adjusting based on the currently executing tasks
				double adjust = 0;
				double exec_so_far = 0;
				if (startTstampsCurrentlyExecBigJobs.containsKey(t_node)) {
					double est_crt = estimateCurrentlyExecBigJobs.get(t_node);
					exec_so_far = System.currentTimeMillis() - startTstampsCurrentlyExecBigJobs.get(t_node);
					// adjust=Math.min(exec_so_far,this.estimatedDuration);
					adjust = Math.max(est_crt - exec_so_far, 0);
					LOG.info("When build pq found node " + t_node + " in startTstampsCurrentlyExecBigJobs exec_so_far: " + exec_so_far + " estimatedDur: " + est_crt
							+ " adjust: " + adjust);
				}
				double total_wait = wait + adjust;

				LOG.info("Build PQ. Node " + isa.toString() + " queue: " + wait + " adjusted: " + adjust + " total_wait: " + total_wait);
				pq.add(new NodeWaitTuple(isa, total_wait));
				resPerNode.put(isa, 0);
			}

			for (int i = 0; i < reservationsToLaunch; i++) {
				NodeWaitTuple nwt = pq.poll();
				nwt.wait += this.estimatedDuration;
				pq.add(nwt);
				int toAssign = resPerNode.get(nwt.node);
				resPerNode.put(nwt.node, toAssign + 1);
			}

			Iterator it = resPerNode.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry pair = (Map.Entry) it.next();
				InetSocketAddress node = (InetSocketAddress) pair.getKey();
				int numReservations = (Integer) pair.getValue();
				it.remove();
				if (numReservations != 0)
					LOG.info("Assigning " + numReservations + " reservations each of " + this.estimatedDuration + " to " + node.toString());
				else
					continue;

				THostPort outstandingNode = new THostPort(node.getAddress().getHostAddress(), node.getPort());
				outstandingReservations.put(outstandingNode, numReservations);
				if (!UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.containsKey(outstandingNode)) {
					UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.put(outstandingNode, numReservations * this.estimatedDuration);
				} else {
					Double d = UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(outstandingNode);
					UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.put(outstandingNode, d + numReservations * this.estimatedDuration);
				}

				TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(schedulingRequest.getApp(), schedulingRequest.getUser(),
						schedulingRequest.shortJob, requestId, schedulerAddress, outstandingNode, numReservations);
				requests.put(node, request);
			}

		}
		return requests;

	}
	
	/* *******************************************************
   EAGLE : piggybacking nodes NOT executing long together with taskReservations  
	 ********************************************************/
	@Override
	public List<String> getNotExecutingLong(Collection<InetSocketAddress> nodes) {
		List<String> notExecutingLong = new ArrayList<String>(); 
		synchronized (UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime) { // PAMELA not sure if needed
			List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
			for (InetSocketAddress isa : nodeList) {
				THostPort t_node = new THostPort(isa.getAddress().getHostAddress(), isa.getPort());
				//LOG.debug("EAGLE getNotExecutingLong t_node" + t_node+" get? wait time " + UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(t_node));
				if (!UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.containsKey(t_node) 
						|| UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(t_node)<=0.0)
					notExecutingLong.add(isa.toString());
			}
		}
		LOG.debug("EAGLE getNotExecutingLong " +notExecutingLong);
		return notExecutingLong;		
	}

	@Override
	public Map<InetSocketAddress, TEnqueueTaskReservationsRequest> getEnqueueTaskReservationsRequests(TSchedulingRequest schedulingRequest, String requestId,
			Collection<InetSocketAddress> nodes, THostPort schedulerAddress) {

		LOG.debug(Logging.functionCall(schedulingRequest, requestId, nodes, schedulerAddress));
		this.schedulingRequest = schedulingRequest;
		int numTasks = schedulingRequest.getTasks().size();
		int reservationsToLaunch = (int) Math.ceil(probeRatio * numTasks);
		LOG.debug("Request " + requestId + ": Creating " + reservationsToLaunch + " task reservations for " + numTasks + " tasks");

		// Get a random subset of nodes by shuffling list.
		List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
		Collections.shuffle(nodeList);
		if (reservationsToLaunch < nodeList.size())
			nodeList = nodeList.subList(0, reservationsToLaunch);

		for (TTaskSpec task : schedulingRequest.getTasks()) {
			TTaskLaunchSpec taskLaunchSpec = new TTaskLaunchSpec(task.getTaskId(), task.bufferForMessage());
			unlaunchedTasks.add(taskLaunchSpec);
		}

		HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();

		int numReservationsPerNode = 1;
		if (nodeList.size() < reservationsToLaunch) {
			numReservationsPerNode = reservationsToLaunch / nodeList.size();
		}
		StringBuilder debugString = new StringBuilder();
		for (int i = 0; i < nodeList.size(); i++) {
			int numReservations = numReservationsPerNode;
			if (reservationsToLaunch % nodeList.size() > i)
				++numReservations;
			InetSocketAddress node = nodeList.get(i);
			debugString.append(node.getAddress().getHostAddress() + ":" + node.getPort() + ","+numReservations);
			debugString.append(";");
			// TODO: this needs to be a count!
			THostPort outstandingNode = new THostPort(node.getAddress().getHostAddress(), node.getPort());
			outstandingReservations.put(outstandingNode, numReservations);
			TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(schedulingRequest.getApp(), schedulingRequest.getUser(),
					schedulingRequest.shortJob, requestId, schedulerAddress, outstandingNode, numReservations);
			requests.put(node, request);
		}
		LOG.debug("Request " + requestId + ": Launching enqueueReservation on " + nodeList.size() + " node monitors: " + debugString.toString());
		return requests;
	}

	@Override
	public List<TTaskLaunchSpec> assignTask(THostPort nodeMonitorAddress, THostPort originalNodeMonitorAddress) {
		Integer numOutstandingReservations = outstandingReservations.get(nodeMonitorAddress);
		if (numOutstandingReservations == null) {
			LOG.debug("Node monitor " + nodeMonitorAddress + " not in list of outstanding reservations, looking up for original node monitor "
					+ originalNodeMonitorAddress);
			// HAWK stealing: case for originalNodeMonitorAddress!!!
			numOutstandingReservations = outstandingReservations.get(originalNodeMonitorAddress);
			nodeMonitorAddress = originalNodeMonitorAddress;
			if (numOutstandingReservations == null)
				return Lists.newArrayList();
		}

		if (numOutstandingReservations == 1) {
			outstandingReservations.remove(nodeMonitorAddress);
			if (scheduler.amIMaster == true) {
				Double d = UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(nodeMonitorAddress);
				UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.put(nodeMonitorAddress, d - this.estimatedDuration);
			}
		} else {
			outstandingReservations.put(nodeMonitorAddress, numOutstandingReservations - 1);
			if (scheduler.amIMaster == true) {
				Double d = UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.get(nodeMonitorAddress);
				UnconstrainedTaskPlacer.outstandingReservationsTotalWaitTime.put(nodeMonitorAddress, d - this.estimatedDuration);
			}
		}

		if (unlaunchedTasks.isEmpty()) {
			LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() + ": Not assigning a task (no remaining unlaunched tasks).");
			return Lists.newArrayList();
		} else {
			TTaskLaunchSpec launchSpec = unlaunchedTasks.get(0);
			unlaunchedTasks.remove(0);
			LOG.debug("Request " + requestId + ", node monitor " + nodeMonitorAddress.toString() + ": Assigning task");

			long start_tstamp = System.currentTimeMillis();
			LOG.info("Task on node: " + nodeMonitorAddress.toString() + " starting at: " + start_tstamp);
			startTstampsCurrentlyExecBigJobs.put(nodeMonitorAddress, start_tstamp);
			estimateCurrentlyExecBigJobs.put(nodeMonitorAddress, this.estimatedDuration);

			return Lists.newArrayList(launchSpec);
		}
	}

	@Override
	public boolean allTasksPlaced() {
		return unlaunchedTasks.isEmpty();
	}

	@Override
	public Set<THostPort> getOutstandingNodeMonitorsForCancellation() {
		if (!cancelled) {
			cancelled = true;
			return outstandingReservations.keySet();
		}
		return new HashSet<THostPort>();
	}

	/** EAGLE */
	@Override
	public Map<InetSocketAddress, TEnqueueTaskReservationsRequest> getRelocateEnqueueTaskRequests(String requestId,
			List<InetSocketAddress> probestoRelocate, Collection<InetSocketAddress> nodes, THostPort schedulerAddress, Boolean roundsLeft) {
		LOG.debug(Logging.functionCall(requestId, probestoRelocate, nodes, schedulerAddress));
		int numTasks = schedulingRequest.getTasks().size();
		
		// Clean outstandingReservations
		int alreadyExecuted = 0;
		for(InetSocketAddress node: probestoRelocate){
			THostPort relocatingNode = new THostPort(node.getAddress().getHostAddress(), node.getPort());
			if(!outstandingReservations.containsKey(relocatingNode)){
				LOG.debug("Request " + requestId + ": Relocating for node " + relocatingNode + " not in outstandingReservations. Deleting it from probes to relocate.");
				alreadyExecuted++;
			}else{				
				Integer numOutstandingReservations = outstandingReservations.get(relocatingNode);
				if (numOutstandingReservations == 1)
					outstandingReservations.remove(relocatingNode);
				else
					outstandingReservations.put(relocatingNode, numOutstandingReservations - 1);
			}
		}
		
		//TODO calculate missing probes to launch 
		int reservationsToLaunch = probestoRelocate.size()-alreadyExecuted; //(int) Math.ceil(probeRatio * numTasks);
		LOG.debug("Request " + requestId + ": Relocating " + reservationsToLaunch + " task reservations for " + numTasks + " tasks. Number of 'free' nodes "+ nodes.size());

		List<InetSocketAddress> nodeList = Lists.newArrayList(nodes);
		if(scheduler.last_round_short_partition && !roundsLeft){
			String app = getSchedulingRequest().getApp();
			nodeList = Lists.newArrayList(scheduler.getAllBackends(app)); // get all nodes somehow!
			Collections.sort(nodeList, new Comparator<InetSocketAddress>() {
				public int compare(InetSocketAddress a1, InetSocketAddress a2) {
					return a1.toString().compareTo(a2.toString());
				}
			});

			LOG.info("Initial node list size: " + nodeList.size());
			int last_nodeID = (int) (nodeList.size() * scheduler.bigPartition / 100);
			nodeList = nodeList.subList(last_nodeID,nodeList.size());
			LOG.info("Using nodes from " + last_nodeID + " to "+ (nodeList.size()-1)+". List consists of : " + nodeList);
		}

		// Get a random subset of nodes by shuffling list.
		Collections.shuffle(nodeList);
		if (reservationsToLaunch < nodeList.size())
			nodeList = nodeList.subList(0, reservationsToLaunch);

		HashMap<InetSocketAddress, TEnqueueTaskReservationsRequest> requests = Maps.newHashMap();

		int numReservationsPerNode = 1;
		if (nodeList.size() < reservationsToLaunch) {
			numReservationsPerNode = reservationsToLaunch / nodeList.size();
		}
		
		StringBuilder debugString = new StringBuilder();
		for (int i = 0; i < nodeList.size(); i++) {
			int numReservations = numReservationsPerNode;
			if (reservationsToLaunch % nodeList.size() > i)
				++numReservations;
			InetSocketAddress node = nodeList.get(i);
			debugString.append(node.getAddress().getHostAddress() + ":" + node.getPort());
			debugString.append(";");
			// TODO: this needs to be a count!
			THostPort outstandingNode = new THostPort(node.getAddress().getHostAddress(), node.getPort());
			int previousOutstandingReservatios =0;
			if(outstandingReservations.containsKey(outstandingNode))
				previousOutstandingReservatios = outstandingReservations.get(outstandingNode);
			outstandingReservations.put(outstandingNode, numReservations+previousOutstandingReservatios);
			TEnqueueTaskReservationsRequest request = new TEnqueueTaskReservationsRequest(schedulingRequest.getApp(), schedulingRequest.getUser(),
					schedulingRequest.shortJob, requestId, schedulerAddress, outstandingNode, numReservations);
			requests.put(node, request);
		}
		LOG.debug("Request " + requestId + ": Launching enqueueReservation on " + nodeList.size() + " node monitors: " + debugString.toString());
		return requests;
	}

	public TSchedulingRequest getSchedulingRequest() {
		return schedulingRequest;
	}
}
