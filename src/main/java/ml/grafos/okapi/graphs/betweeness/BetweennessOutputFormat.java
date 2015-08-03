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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Set;


/**
 * Writes the approximated betweenness value for each vertex
 */
public class BetweennessOutputFormat extends TextVertexOutputFormat<Text, BetweenessData, Text> {

    private HashFunction hf = Hashing.md5();

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new SBVertexWriter();
    }

    public class SBVertexWriter extends TextVertexWriter {

        // return betweenes and cluster id
        public void writeVertex(Vertex<Text, BetweenessData, Text> vertex) throws IOException, InterruptedException {
            BetweenessData resultdata = vertex.getValue();

            // get cluster id
            Set<String> inferredNeighbours = resultdata.getPathDataMap().keySet();
            String id = null;

            for(String neighbour : inferredNeighbours) {
                id = minString(id , neighbour);
            }

            HashCode resultID = hf.newHasher().putString(id).hash();

            // FORMAT: <URL> <NUMBEROFSHORTESTPATHS> <BETWEENESSRRESULT> <CLUSTERID> <NUMBEROFMEMBERS>
            getRecordWriter().write(new Text(vertex.getId().toString()), new Text(resultdata.getNumPaths().toString() + ", " + String.format("%.32f", resultdata.getBetweenness())   + ", " + resultID.toString() + ", " + resultdata.getPathDataMap().size()));
        }
    }

    private String minString(String a, String b) {
        if(a == null && b != null) {
            return b;
        } else if (a != null && b == null ) {
            return a;
        } else if(a == null && b == null) {
            return null;
        }

        int compare = a.compareTo(b);
        // b is smaller than a
        if(compare < 0) {
            return a;
            // a is smaller than v
        } else if(compare > 0) {
            return b;
            // both are equal
        } else {
            return a;
        }
    }
}
