/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package ml.grafos.okapi.graphs.betweeness;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;
import java.util.Map.Entry;


/**
 * Calculates Shortest paths and calculates Betweeenes Centrality.
 * Use with BetweenessMasterCompute class.
 */
public class BetweenessComputation extends AbstractComputation<Text, BetweenessData, Text, ShortestPathData, ShortestPathData> {

    private static final Logger logger = LoggerFactory.getLogger(BetweenessComputation.class);

    @Override
    public void initialize(GraphState graphState, WorkerClientRequestProcessor<Text, BetweenessData, Text> workerClientRequestProcessor, GraphTaskManager<Text, BetweenessData, Text> graphTaskManager, WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
        super.initialize(graphState, workerClientRequestProcessor, graphTaskManager, workerGlobalCommUsage, workerContext);
    }

    @Override
    public void compute(Vertex<Text, BetweenessData, Text> vertex, Iterable<ShortestPathData> messages) throws IOException {

        long step = this.getSuperstep();
        State state = getCurrentGlobalState();
        int updateCount = 0;
        String id = vertex.getId().toString();
        Map<String, ArrayList<String>> combinedMessages = new HashMap<>();

        if (step == 0) {
            vertex.setValue(new BetweenessData());
        }

        BetweenessData vertexValue = vertex.getValue();

        switch (state) {
            case SHORTEST_PATH_START:
                 // start shortest path processing from this vertex as source
                sendMessageToAllEdges(vertex, ShortestPathData.createShortestPathMessage(id, id, 1));
                // add reflexiv path
                vertexValue.addPathData(ShortestPathData.createShortestPathMessage(id, id, 0));

                this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));

                break;

            case SHORTEST_PATH_RUN:
                Map<String, ShortestPathList> updatedPathMap = new HashMap<String, ShortestPathList>();

                // process incoming messages
                for (ShortestPathData message : messages) {
                    ShortestPathList updatedPath = vertexValue.addPathData(message);
                    if (updatedPath != null) {
                        updatedPathMap.put(message.getSource(), updatedPath);
                    }
                }

                // send outgoing messages for each updated shortest path
                for (Entry<String, ShortestPathList> entry : updatedPathMap.entrySet()) {
                    sendMessageToAllEdges(vertex, ShortestPathData.createShortestPathMessage(entry.getKey(), id, entry.getValue().getDistance() + 1));
                    updateCount++;
                }

                if(updateCount > 0 ) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
                }

                break;

            case BETWEENESSS_PING:
                // ping the predecessors that they are involved in a shortest path
                Map<String, ShortestPathList> shortestPathMap = vertexValue.getPathDataMap();

                // dont send the message directly, combine messages to common neighbour
                for(Entry<String, ShortestPathList> entry : shortestPathMap.entrySet()) {
                    String source = entry.getKey();

                    // only ping if this node isn't directly the source
                    if(!id.equals(source)){
                        ShortestPathList pathes = entry.getValue();

                        for (String predecessor : pathes.getPredecessors()) {
                            if(!combinedMessages.containsKey(predecessor)) {
                                combinedMessages.put(predecessor, new ArrayList<String>());
                            }
                            combinedMessages.get(predecessor).add(source);
                            updateCount++;
                        }
                    }
                }

                for(String predecessor : combinedMessages.keySet()){
                    sendMessage(new Text(predecessor), ShortestPathData.getPingMessage(combinedMessages.get(predecessor), id));
                }
                combinedMessages.clear();

                if(updateCount > 0 ) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                }

                break;
            case BETWEENESSS_POPULATE:
                // save number of shortest path which this node is involved in and populate the info to the next appropriate predecessor
                Map<String, ShortestPathList> shortestPaths = vertexValue.getPathDataMap();


                int shortestPathcount = 0;
                // process incoming messages
                for (ShortestPathData message : messages) {
                    // this is a combined message. iterate over each list entry
                    for(String source : message.getShortestPathSources()){
                        // each combined entry means this node is involved in a shortest path
                        ++shortestPathcount;

                        // dont send the message directly, combine messages to common neighbour
                        // notify the next predecessors that it is involved in a shortest path for the specified source if we are not directly the source
                        if(!id.equals(source)) {
                            ShortestPathList shortestPathList = shortestPaths.get(source);
                            for (String predecessor : shortestPathList.getPredecessors()) {
                                if(!combinedMessages.containsKey(predecessor)) {
                                    combinedMessages.put(predecessor, new ArrayList<String>());
                                }
                                combinedMessages.get(predecessor).add(source);
                                updateCount++;
                            }
                        }
                    }
                }

                // send combined messages
                for(String predecessor : combinedMessages.keySet()){
                    sendMessage(new Text(predecessor), ShortestPathData.getPingMessage(combinedMessages.get(predecessor), id));
                }
                combinedMessages.clear();

                if(shortestPathcount > 0) {
                    vertexValue.addNumberofPaths(BigInteger.valueOf(shortestPathcount));
                }

                if(updateCount > 0) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(1));
                }

                break;

            case BETWEENESSS_CALCULATE:
                // TODO calculate correct number of shortest pathes in this component (e.g. send number of shortestpaths to each node in cluster)

                // becaus of all-pairshortest path we have a shortest path for each node in the cluster. so we can get the number of vertices in this cluster
                BigInteger totalNumberOfVertices = BigInteger.valueOf(vertexValue.getPathDataMap().size());
                BigInteger numberOfShortestPathesInCluster =  totalNumberOfVertices.multiply(totalNumberOfVertices);

                if(numberOfShortestPathesInCluster.equals(BigInteger.ZERO)) {
                    vertexValue.setBetweenness(0.0);
                } else {
                    BigDecimal result = new BigDecimal(vertexValue.getNumPaths()).divide(new BigDecimal(numberOfShortestPathesInCluster), 10, RoundingMode.HALF_DOWN) ;
                    vertexValue.setBetweenness(result.doubleValue());
                }

                // calculate Closeness centrality
                long closenessCentrality = 0;
                for(String source : vertexValue.getPathDataMap().keySet()){
                    closenessCentrality += vertexValue.getPathDataMap().get(source).getDistance();
                }

                vertexValue.setCloseness(closenessCentrality);

                // calculate avg shortest path length
                long sumShortestPath = 0;
                int numberOfShortestPath = 0;
                for(String source : vertexValue.getPathDataMap().keySet()){
                    ShortestPathList shortestPathList = vertexValue.getPathDataMap().get(source);
                    for(String pred: shortestPathList.getPredecessors()){
                        sumShortestPath += shortestPathList.getDistance();
                        numberOfShortestPath++;
                    }
                }

                vertexValue.setAvgShortestPathDistance((double)sumShortestPath/(double)numberOfShortestPath);

                break;
        }
    }


    /**
     * Return the current global state
     *
     * @return State that stores the current global state
     */
    private State getCurrentGlobalState() {
        IntWritable stateInt = this.getAggregatedValue(BetweenessMasterCompute.STATE_AGG);
        return State.values()[stateInt.get()];
    }
}
