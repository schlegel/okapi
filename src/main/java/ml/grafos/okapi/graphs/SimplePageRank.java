/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

/**
 * Basic Pregel PageRank implementation.
 *
 * This version initializes the value of every vertex to 1/N, where N is the
 * total number of vertices.
 *
 * The maximum number of supersteps is configurable.
 */
public class SimplePageRank extends BasicComputation<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    /** Default number of supersteps */
    public static final int MAX_SUPERSTEPS_DEFAULT = 41;
    /** Property name for number of supersteps */
    public static final String MAX_SUPERSTEPS = "pagerank.max.supersteps";

    /** Logger */
    private static final Logger LOG =
            Logger.getLogger(SimplePageRank.class);

    @Override
    public void compute(Vertex<IntWritable, DoubleWritable, IntWritable> vertex, Iterable<DoubleWritable> messages) {


        if (getSuperstep() == 0) {
            // vertex can have only outgoing edges. so we cant access the value of the edges. send value so they get it in superstep 1
            if(vertex.getEdges().iterator().hasNext()) {
                int numberVerticesInCluster = vertex.getEdges().iterator().next().getValue().get();
                sendMessageToAllEdges(vertex, new DoubleWritable(numberVerticesInCluster));
            }
        }

        if (getSuperstep() == 1) {
            double numberVerticesInCluster;

            // number of vertices in cluster is either in the edge data or in an incoming message
            if(vertex.getEdges().iterator().hasNext()) {
                numberVerticesInCluster = vertex.getEdges().iterator().next().getValue().get();
            } else {
                numberVerticesInCluster = messages.iterator().next().get();
            }

            vertex.setValue(new DoubleWritable(numberVerticesInCluster));

            long edges = vertex.getNumEdges();
            sendMessageToAllEdges(vertex, new DoubleWritable((1f / numberVerticesInCluster) / edges));
        }

        if (getSuperstep() > 1) {
            double sum = 0;
            for (DoubleWritable message : messages) {
                sum += message.get();
            }

            // vertext value holds the number of vertices in the cluster
            DoubleWritable pageRankValue = new DoubleWritable((0.15f / vertex.getValue().get()) + 0.85f * sum);

            if (getSuperstep() < getContext().getConfiguration().getInt(MAX_SUPERSTEPS, MAX_SUPERSTEPS_DEFAULT)) {
                long edges = vertex.getNumEdges();
                sendMessageToAllEdges(vertex, new DoubleWritable(pageRankValue.get() / edges));
            } else {
                // set vertex value to the actual pagerank value
                vertex.setValue(pageRankValue);
                vertex.voteToHalt();
            }
        }
    }
}
