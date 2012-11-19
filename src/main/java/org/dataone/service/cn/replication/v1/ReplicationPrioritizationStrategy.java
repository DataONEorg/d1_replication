/**
 * This work was created by participants in the DataONE project, and is
 * jointly copyrighted by participating institutions in DataONE. For
 * more information on DataONE, see our web site at http://dataone.org.
 *
 *   Copyright 2012. All rights reserved.
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
 * 
 * $Id: ReplicationManager.java 10445 2012-10-22 14:41:43Z sroseboo $
 */

package org.dataone.service.cn.replication.v1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.cn.dao.DaoFactory;
import org.dataone.cn.dao.exceptions.DataAccessException;
import org.dataone.configuration.Settings;
import org.dataone.service.types.v1.Identifier;
import org.dataone.service.types.v1.NodeReference;
import org.dataone.service.types.v1.Replica;
import org.dataone.service.types.v1.ReplicationPolicy;
import org.dataone.service.types.v1.ReplicationStatus;
import org.dataone.service.types.v1.SystemMetadata;

/**
 * ReplicationPrioritizationStrategy encapsulates the logic and calculations
 * used to prioritize target Member Nodes as replication targets.
 * 
 * Used as a delegate by ReplicationManager.
 * 
 * @author cjones
 * 
 */
public class ReplicationPrioritizationStrategy {

    public static Log log = LogFactory
            .getLog(ReplicationPrioritizationStrategy.class);

    /* The number of concurrent outstanding requests to a target member node */
    private int requestLimit = 10;
    
    /* The number of concurrent outstanding requests to a target member node */
    private float successThreshold = 0.8f;

    /* flag to switch use of the outstanding request factor in prioritization */
    private boolean useRequestFactor = true;
    
    /* flag to switch use of the failure factor in prioritization */
    private boolean useFailureFactor = true;

    /* flag to switch use of the bandwidth factor in prioritization */
    private boolean useBandwidthFactor = true;

    /* flag to switch use of the preference factor in prioritization */
    private boolean usePreferenceFactor = true;

    /**
     * Constructor: create a prioritization strategy
     */
    public ReplicationPrioritizationStrategy() {
        // load prioritization settings
        this.requestLimit = 
            Settings.getConfiguration().getInt("replication.concurrent.request.limit");
        this.successThreshold = 
            Settings.getConfiguration().getFloat("replication.success.threshold");
        this.useRequestFactor = 
            Settings.getConfiguration().getBoolean("replication.prioritization.useRequestFactor");
        this.useFailureFactor = 
            Settings.getConfiguration().getBoolean("replication.prioritization.useFailureFactor");
        this.useBandwidthFactor = 
            Settings.getConfiguration().getBoolean("replication.prioritization.useBandwidthFactor");
        this.usePreferenceFactor = 
            Settings.getConfiguration().getBoolean("replication.prioritization.usePreferenceFactor");
    }

    /**
     * For the given node list, report the pending request factor of each node.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return requestFactors the pending request factors of the nodes
     */
    public Map<NodeReference, Float> getPendingRequestFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {

        // TODO: implement the useCache parameter, ignored for now

        // A map to store the raw pending replica counts
        Map<NodeReference, Integer> pendingRequests = new HashMap<NodeReference, Integer>();
        // A map to store the current request factors per node
        Map<NodeReference, Float> requestFactors = new HashMap<NodeReference, Float>();

        /*
         * See http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Replication Requests Factor R ----------------------------- The goal
         * here is to be sure not to overload nodes by only issuing a fixed
         * number of requests for replication to a given member node. If the
         * request limit is reached, don't submit more requests.
         * 
         * Max request limit (rl) Number of pending replication tasks on target
         * (rt)
         * 
         * R = 1 if rt < rl, 0 otherwise Also want to deal wiht the number of
         * requests pending against a source node, but defer until later:
         * 
         * Number of pending replication tasks on source (rs) To be determined
         * -- refactor R including rs
         */

        // query the systemmetadatastatus table to get counts of queued and
        // requested replicas by node identifier
        try {
            pendingRequests = DaoFactory.getReplicationDao()
                    .getPendingReplicasByNode();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve pending replicas by node: "
                    + dataAccessEx.getMessage());
        }
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        // determine results for each MN in the list
        while (nodeIterator.hasNext()) {
            NodeReference nodeId = nodeIterator.next();

            // get the failures for the node
            Integer pending = (pendingRequests.get(nodeId) != null) ? pendingRequests
                    .get(nodeId) : new Integer(0);
            log.debug("Pending requests for node " + nodeId.getValue() + " is "
                    + pending.intValue());

            if (pending.intValue() <= this.requestLimit) {
                // currently under or equal to the limit
                requestFactors.put(nodeId, new Float(1));

            } else {
                // currently over the limit
                requestFactors.put(nodeId, new Float(0));
                log.info("Node " + nodeId.getValue()
                        + " is currently over its request limit of "
                        + this.requestLimit + " requests.");

            }

        }
        return requestFactors;
    }

    /**
     * For the given node list, report the success factor as a surrogate for the
     * nodes' demonstrated replication successes over a recent time period.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return failureFactors the failure factors of the nodes
     */
    public Map<NodeReference, Float> getFailureFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {
        // A map to store the raw failed replica counts
        Map<NodeReference, Integer> failedRequests = new HashMap<NodeReference, Integer>();
        // A map to store the raw completed replica counts
        Map<NodeReference, Integer> completedRequests = new HashMap<NodeReference, Integer>();
        // A map to store the current failure factors per node
        HashMap<NodeReference, Float> failureFactors = new HashMap<NodeReference, Float>();
        Float successThreshold = new Float(this.successThreshold);
        Float failureFactor;
        /*
         * See http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Failure Factor F ---------------- The goal here is to avoid nodes
         * that are failing a lot, and for those that are failing less than an
         * arbitrary threshold, prioritize them proportionally to their success
         * rate.
         * 
         * Number of replication successes over last 3 days (ps) Number of
         * replication failures over last 3 days (pf) days)
         * 
         * Success threshold (st) = default 0.80 F = 0 if ps/(ps+pf) <= st, else
         * ps/(ps+pf)
         */
        try {
            failedRequests = DaoFactory.getReplicationDao()
                    .getRecentFailedReplicas();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve recent failed replicas by node: "
                    + dataAccessEx.getMessage());
        }
        try {
            completedRequests = DaoFactory.getReplicationDao()
                    .getRecentCompletedReplicas();
        } catch (DataAccessException dataAccessEx) {
            log.error("Unable to retrieve recent completed replicas by node: "
                    + dataAccessEx.getMessage());
        }
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        while (nodeIterator.hasNext()) {
            NodeReference nodeId = nodeIterator.next();

            // get the failures for the node
            Integer failures = (failedRequests.get(nodeId) != null) ? failedRequests
                    .get(nodeId) : new Integer(0);
            // get the successes for the node
            Integer successes = (completedRequests.get(nodeId) != null) ? completedRequests
                    .get(nodeId) : new Integer(0);

            // in the case there's no real stats
            if (failures.intValue() == 0 && successes.intValue() == 0) {
                // bootstrap the MN as a medium-performant node
                failureFactors.put(nodeId, new Float(1.0f));

            } else {
                // for MNs that are young, give 'em 5 attempts before calculating
                if ( failedRequests.size() + completedRequests.size() < 5 ) {
                    failureFactor = new Float(1.0f); 
                    log.debug("Gave node " + nodeId.getValue() + " a pass " +
                        "since it has less than 5 replica attempts.");
                    
                } else {
                    // calculate the failure factor
                    failureFactor = new Float(successes.floatValue()
                            / (successes.floatValue() + failures.floatValue()));
                    if ( failureFactor <= successThreshold ) {
                        failureFactor = new Float(0.0f);
                        
                    }
                }
                failureFactors.put(nodeId, failureFactor);

            }

        }

        return failureFactors;
    }

    /**
     * For the given nodes, return the bandwidth factor as a surrogate for the
     * nodes' demonstrated throughput over a recent time period.
     * 
     * @param nodeIdentifiers
     *            the list of nodes to include in the report
     * @param useCache
     *            use the cached values if the cache hasn't expired
     * @return bandwidthFactors the bandwidth factor of the node
     */
    public Map<NodeReference, Float> getBandwidthFactors(
            List<NodeReference> nodeIdentifiers, boolean useCache) {
        HashMap<NodeReference, Float> bandwidthFactors = new HashMap<NodeReference, Float>();

        /*
         * TODO: calculate the bandwidth factor based on the following notes at
         * http://epad.dataone.org/20120420-replication-priority-queue
         * 
         * Bandwith Factor B ----------------- The goal here is to utilize high
         * bandwidth nodes more than low by skewing the rank in favor of high
         * bandwidth nodes. We do this by calculating B from 0 to 2 and
         * multiplying the other metrics by B, which will proportionally reduce
         * or enhance the rank based on B. THe metric following uses the range
         * of bandwidths available across all nodes to determine B such that the
         * lowest bandwidth nodes will be near zero and the highest near 2, but
         * a lot of the nodes will cluster around 1 due to the log functions.
         * 
         * B = 2*(log(b/bmin) / log(bmax/bmin))
         * 
         * will range from 0 to 2 Node Bandwidth b MaxNodeBandwith bmax
         * MinNodeBandwidth bmin
         * 
         * Note that its not clear how we actually estimate node bandwidth -- is
         * it a node reported metadata value, or something we measure during
         * normal operations? The latter would be possible by recording the time
         * to replicate data between two nodes and dividing by the replica size,
         * and assign the resultant value to both nodes -- over time an average
         * would build up indicating the effective throughput that considers not
         * just network bandwidth but also storage I/O rates and admin overhead.
         */

        // Placeholder code: assign equal bandwidth factors for now
        Iterator<NodeReference> nodeIterator = nodeIdentifiers.iterator();

        while (nodeIterator.hasNext()) {
            bandwidthFactors.put((NodeReference) nodeIterator.next(),
                    new Float(1.0f));
        }

        return bandwidthFactors;
    }

    /**
     * Prioritize a list of potential replica target nodes based on a number of
     * factors including preferred/blocked node lists, pending request, failure,
     * and bandwidth factors.
     * 
     * @param sysmeta
     * @param potentialNodeList
     * 
     * @return nodesByPriority a list of nodes by descending priority
     */
    @SuppressWarnings("unchecked")
    public List<NodeReference> prioritizeNodes(
            List<NodeReference> potentialNodeList, SystemMetadata sysmeta) {
        List<NodeReference> nodesByPriority = new ArrayList<NodeReference>();
        ReplicationPolicy replicationPolicy = sysmeta.getReplicationPolicy();
        Identifier pid = sysmeta.getIdentifier();
        Map<NodeReference, Float> requestFactorMap = new HashMap<NodeReference, Float>();
        Map<NodeReference, Float> failureFactorMap = new HashMap<NodeReference, Float>();
        Map<NodeReference, Float> bandwidthFactorMap = new HashMap<NodeReference, Float>();

        log.info("Retrieving performance metrics for the potential replication list for "
                + pid.getValue());

        // get performance metrics for the potential node list
        requestFactorMap = getPendingRequestFactors(potentialNodeList, false);
        failureFactorMap = getFailureFactors(potentialNodeList, false);
        bandwidthFactorMap = getBandwidthFactors(potentialNodeList, false);

        // get the preferred list, if any
        List<NodeReference> preferredList = null;
        if (replicationPolicy != null) {
            preferredList = replicationPolicy.getPreferredMemberNodeList();

        }

        // get the blocked list, if any
        List<NodeReference> blockedList = null;
        if (replicationPolicy != null) {
            blockedList = replicationPolicy.getBlockedMemberNodeList();

        }

        Map<NodeReference, Float> nodeScoreMap = new HashMap<NodeReference, Float>();
        ValueComparator valueComparator = new ValueComparator(nodeScoreMap);
        TreeMap<NodeReference, Float> sortedScoresMap = new TreeMap<NodeReference, Float>(
                valueComparator);
        Iterator<NodeReference> nodeIterator = potentialNodeList.iterator();

        // iterate through the potential node list and calculate performance
        // scores
        while (nodeIterator.hasNext()) {
            NodeReference nodeId = (NodeReference) nodeIterator.next();
            Float preferenceFactor = 1.0f; // default preference for all nodes

            // increase preference for preferred nodes
            if (preferredList != null && preferredList.contains(nodeId)) {
                preferenceFactor = 2.0f;

            }

            // decrease preference for preferred nodes
            if (blockedList != null && blockedList.contains(nodeId)) {
                preferenceFactor = 0.0f;

            }
            log.debug("Node " + nodeId.getValue() + " preferenceFactor is "
                    + preferenceFactor);

            Float nodePendingRequestFactor = 1.0f;
            if (requestFactorMap.get(nodeId) != null) {
                nodePendingRequestFactor = requestFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " requestFactor is "
                        + nodePendingRequestFactor);

            }

            Float nodeFailureFactor = 1.0f;
            if (failureFactorMap.get(nodeId) != null) {
                nodeFailureFactor = failureFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " failureFactor is "
                        + nodeFailureFactor);

            }

            Float nodeBandwidthFactor = 1.0f;
            if (bandwidthFactorMap.get(nodeId) != null) {
                nodeBandwidthFactor = bandwidthFactorMap.get(nodeId);
                log.debug("Node " + nodeId.getValue() + " bandwidthFactor is "
                        + nodeBandwidthFactor);

            }

            // Score S = R * F * B * P
            // (any zero score removes node from the list)
            if ( ! this.useRequestFactor ) {
                nodePendingRequestFactor = 1.0f;
                log.debug("useRequestFactor is false, using 1.0");
            }
            if ( ! this.useFailureFactor ) {
                nodeFailureFactor = 1.0f; 
                log.debug("useFailureFactor is false, using 1.0");
            }
            if ( ! this.useBandwidthFactor ) {
                nodeBandwidthFactor = 1.0f; 
                log.debug("useBandwidthFactor is false, using 1.0");
            }
            if ( ! this.usePreferenceFactor ) {
                preferenceFactor = 1.0f; 
                log.debug("usePreferenceFactor is false, using 1.0");
            }

            Float score = nodePendingRequestFactor * nodeFailureFactor
                    * nodeBandwidthFactor * preferenceFactor;
            log.debug("Score for " + nodeId.getValue() + " will be "
                    + nodePendingRequestFactor + " * " + nodeFailureFactor
                    + " * " + nodeBandwidthFactor + " * " + preferenceFactor);

            // if the node is already listed and is pending or complete,
            // zero its score (to avoid repeat replica tasks)
            List<Replica> replicaList = sysmeta.getReplicaList();
            for (Replica replica : replicaList) {
                String nodeIdStr = replica.getReplicaMemberNode().getValue();
                ReplicationStatus nodeStatus = replica.getReplicationStatus();
                if (nodeIdStr == nodeId.getValue() && 
                   (nodeStatus == ReplicationStatus.QUEUED || 
                    nodeStatus == ReplicationStatus.REQUESTED || 
                    nodeStatus == ReplicationStatus.COMPLETED)) {
                    score = new Float(0.0f);
                    log.debug("Node " + nodeId.getValue()
                            + " is already listed " + "as a "
                            + nodeStatus.toString() + " replica for identifier"
                            + pid.getValue());
                    break;
                }
            }
            log.info("Priority score for " + nodeId.getValue() + " is "
                    + score.floatValue());
            nodeScoreMap.put(nodeId, score);

        }

        sortedScoresMap.putAll(nodeScoreMap);

        log.debug("Sorted scores map size: " + sortedScoresMap.size());

        Set<Float> usedScores = new HashSet<Float>();
        List<NodeReference> randomSortedNodes = new ArrayList<NodeReference>();
        for (NodeReference nodeRef : sortedScoresMap.keySet()) {
            Float value = sortedScoresMap.get(nodeRef);
            log.debug("score for node: " + nodeRef.getValue() + " is: " + value.floatValue());
            if (value.floatValue() <= 0) {
                log.info("Removed " + nodeRef.getValue() + ", score is " + value.floatValue());
                continue;
            }
            if (usedScores.contains(value)) {
                continue;
            }
            usedScores.add(value);
            List<NodeReference> sameScores = new ArrayList<NodeReference>();
            for (NodeReference matchRef : sortedScoresMap.keySet()) {
                if (matchRef.equals(nodeRef)) {
                    continue;
                }
                Float matchValue = sortedScoresMap.get(matchRef);
                if (matchValue == null) {
                    log.debug("match value is null for: " + matchRef.getValue());
                } else if (matchValue.floatValue() == value.floatValue()) {
                    log.debug("match value score for node: " + matchRef.getValue() + " is: "
                            + matchValue.floatValue());
                    sameScores.add(nodeRef);
                }
            }
            if (sameScores.isEmpty()) {
                log.debug("Adding " + nodeRef.getValue() + " , score is " + value.floatValue());
                randomSortedNodes.add(nodeRef);
            } else {
                Collections.shuffle(sameScores);
                if (log.isDebugEnabled()) {
                    log.debug("Adding: ");
                    for (NodeReference logRef : sameScores) {
                        log.debug("Node: " + logRef.getValue() + " , score is "
                                + value.floatValue());
                    }
                }
                randomSortedNodes.addAll(sameScores);
            }
        }

        // add sorted map entries to the sorted potential node list
        // log.debug("Sorted scores map size: " + sortedScoresMap.size());
        // if (sortedScoresMap.size() > 0) {
        // log.debug("Sorted scores members: ");
        // for (Entry<NodeReference, Float> entry : sortedScoresMap.entrySet())
        // {
        // if (entry.getValue().floatValue() > 0) {
        // nodesByPriority.add(entry.getKey()); // append to retain
        // // order
        // log.debug("Node: " + entry.getKey().getValue()
        // + ", score: " + entry.getValue().floatValue());
        //
        // } else {
        // log.info("Removed " + entry.getKey().getValue()
        // + ", score is " + entry.getValue().floatValue());
        // }
        // }
        // }
        return randomSortedNodes;
    }

    /**
     * A comparator class used to compare map values for sorting by value
     * 
     * @author cjones
     * 
     */
    private class ValueComparator implements Comparator {
        Map incomingMap;

        /**
         * Constructor - creates the map value comparator instnace
         * 
         * @param incomingMap
         */
        public ValueComparator(Map incomingMap) {
            this.incomingMap = incomingMap;

        }

        /**
         * Compare object values in the map
         * 
         * @return integer showing a is less than, equal to, or greater than b
         */
        public int compare(Object a, Object b) {
            if ((Float) incomingMap.get(a) < (Float) incomingMap.get(b)) {
                return 1;

            } else if ((Float) incomingMap.get(a) == (Float) incomingMap.get(b)) {
                return 0;

            } else {
                return -1;

            }
        }
    }
}
