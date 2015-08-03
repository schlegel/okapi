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
import java.util.HashMap;
import java.util.Map;
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
        String id = vertex.getId().toString();
        int updateCount = 0;
        State state = getCurrentGlobalState();

        if (step == 0) {
            vertex.setValue(new BetweenessData());
        }

        BetweenessData vertexValue = vertex.getValue();

        switch (state) {
            case SHORTEST_PATH_START:

                 // start shortest path processing from this vertex as source
                sendMessageToAllEdges(vertex, ShortestPathData.createShortestPathMessage(id, id, BigInteger.valueOf(1)));
                // add reflexiv path
                vertexValue.addPathData(ShortestPathData.createShortestPathMessage(id, id, BigInteger.valueOf(0)));

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
                // TODO maybe dont sent to nodes which are responsible for the update
                for (Entry<String, ShortestPathList> entry : updatedPathMap.entrySet()) {
                    ShortestPathList spl = entry.getValue();
                    String src = entry.getKey();

                    BigInteger newDistance = spl.getDistance().add(BigInteger.valueOf(1));
                    sendMessageToAllEdges(vertex, ShortestPathData.createShortestPathMessage(src, id, newDistance));

                    updateCount++;
                }

                if(updateCount > 0 ) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
                }

                break;

            case BETWEENESSS_PING:
                // TODO aggregate avg distance in this step
                // ping the predecessors that they are involved in a shortest path
                Map<String, ShortestPathList> shortestPathMap = vertexValue.getPathDataMap();

                for(Entry<String, ShortestPathList> entry : shortestPathMap.entrySet()) {
                    String source = entry.getKey();
                    ShortestPathList pathes = entry.getValue();

                    // only ping if this node isn't directly the source
                    if(!id.equals(source)){
                        for (String predecessor : pathes.getPredecessors()) {
                            sendMessage(new Text(predecessor), ShortestPathData.getPingMessage(source, id));
                            updateCount++;
                        }
                    }
                }

                if(updateCount > 0 ) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
                }

                break;
            case BETWEENESSS_POPULATE:
                // save number of shortest path which this node is involved in and populate the info to the next appropriate predecessor
                Map<String, ShortestPathList> shortestPaths = vertexValue.getPathDataMap();

                int shortestPathcount = 0;
                // process incoming messages
                for (ShortestPathData message : messages) {
                    // each message means this node is involved in a shortest path
                    ++shortestPathcount;

                    // notify the next predecessors that it is involved in a shortest path for the specified source if we are not directly the source
                    if(!id.equals(message.getSource())) {
                        ShortestPathList shortestPathList = shortestPaths.get(message.getSource());
                        for (String predecessor : shortestPathList.getPredecessors()) {
                            updateCount++;
                            sendMessage(new Text(predecessor), ShortestPathData.getPingMessage(message.getSource(), message.getFrom()));
                        }
                    }
                }

                if(shortestPathcount > 0) {
                    vertexValue.addNumberofPaths(BigInteger.valueOf(shortestPathcount));
                }

                if(updateCount > 0) {
                    this.aggregate(BetweenessMasterCompute.UPDATE_COUNT_AGG, new IntWritable(updateCount));
                }

                break;

            case BETWEENESSS_CALCULATE:

                // becaus of all-pairshortest path we have a shortest path for each node in the cluster. so we can get the number of vertices in this cluster
                BigInteger totalNumberOfVertices = BigInteger.valueOf(vertexValue.getPathDataMap().size());
                BigInteger numberOfShortestPathesInCluster =  totalNumberOfVertices.multiply(totalNumberOfVertices);

                if(numberOfShortestPathesInCluster.equals(BigInteger.ZERO)) {
                    vertexValue.setBetweenness(0.0);
                } else {
                    BigDecimal result = new BigDecimal(vertexValue.getNumPaths()).divide(new BigDecimal(numberOfShortestPathesInCluster), 10, RoundingMode.HALF_DOWN) ;
                    vertexValue.setBetweenness(result.doubleValue());
                }

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
