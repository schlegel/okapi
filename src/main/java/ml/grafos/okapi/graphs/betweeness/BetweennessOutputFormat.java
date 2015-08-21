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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Set;


/**
 * Writes the approximated betweenness value for each vertex
 */
public class BetweennessOutputFormat extends TextVertexOutputFormat<IntWritable, BetweenessData, IntWritable> {
    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SBVertexWriter();
    }

    public class SBVertexWriter extends TextVertexWriter {
        @Override
        public void writeVertex(Vertex<IntWritable, BetweenessData, IntWritable> vertex) throws IOException, InterruptedException {
            BetweenessData resultdata = vertex.getValue();

            // get cluster id
            Set<Integer> inferredNeighbours = resultdata.getPathDataMap().keySet();
            Integer clusterId = Integer.MAX_VALUE;

            for(Integer neighbour : inferredNeighbours) {
                clusterId = Math.min(clusterId , neighbour);
            }

            // FORMAT: <ID> <NUMBEROFSHORTESTPATHS> <BETWEENESSRRESULT> <CLUSTERID> <NUMBEROFMEMBERS> <CLOSENESS> <AVGSHORTESTPATHLENGTH>
            getRecordWriter().write(new Text(vertex.getId().toString()), new Text(resultdata.getNumPaths().toString() + ", " + String.format("%.32f", resultdata.getBetweenness())   + ", " + clusterId + ", " + resultdata.getPathDataMap().size() + ", " +  resultdata.getCloseness() + ", " + String.format("%.4f", resultdata.getAvgShortestPathDistance())));
        }
    }
}
