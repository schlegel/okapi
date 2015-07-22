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
package ml.grafos.okapi.graphs.hbse;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.InMemoryVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.giraph.utils.TestGraph;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.*;


public class HBSEComputeTest {
    public GiraphConfiguration getConf() {
        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setMasterComputeClass(HBSEMasterCompute.class);
        conf.setComputationClass(HBSEComputation.class);
        conf.setVertexOutputFormatClass(InMemoryVertexOutputFormat.class);
        conf.set(HBSEConfigurationConstants.BETWEENNESS_OUTPUT_DIR, "tmp/output");
        conf.set(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY, "1");
        conf.set(HBSEConfigurationConstants.BETWEENNESS_SET_MAX_SIZE, "10");
        conf.set(HBSEConfigurationConstants.BETWEENNESS_SET_STABILITY_COUNTER, "3");
        conf.set(HBSEConfigurationConstants.PIVOT_BATCH_SIZE, "2");
        conf.set(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT, "6");
        conf.set("giraph.zkList", "10.211.55.7:2181");
        conf.set("giraph.zkServerPort", "2181");
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("mapreduce.jobtracker.address", "10.211.55.7:543");
        return conf;
    }

    @Test
    public void testComputeOutput() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, VertexData, Text> input = getFirstTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, VertexData, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertTrue(output.getVertex(new Text("2")).getValue().getApproxBetweenness() > 0.0);
        assertTrue(output.getVertex(new Text("1")).getValue().getApproxBetweenness() > 0.0);
        assertEquals(output.getVertex(new Text("3")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("4")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("5")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("6")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("7")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("8")).getValue().getApproxBetweenness(), 0.0, 0.0);
    }

    @Test
    public void testGraphWithShortestPathOne() throws Exception {
        GiraphConfiguration conf = getConf();
        TestGraph<Text, VertexData, Text> input = getShortestPathOneTestGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, VertexData, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(8, output.getVertices().size());
        assertNotSame(output.getVertex(new Text("1")).getValue().getApproxBetweenness(), 0.0);
        assertEquals(output.getVertex(new Text("2")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("3")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("4")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("5")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("6")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("7")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("8")).getValue().getApproxBetweenness(), 0.0, 0.0);

    }

    @Test
    public void testTwoCriticalPointGraph() throws Exception {
        GiraphConfiguration conf = getConf();
        conf.set(HBSEConfigurationConstants.TOTAL_PIVOT_COUNT, "10");
        TestGraph<Text, VertexData, Text> input = getTwoCriticalPointGraph(conf);
        InMemoryVertexOutputFormat.initializeOutputGraph(conf);
        InternalVertexRunner.run(conf, input);
        TestGraph<Text, VertexData, Text> output = InMemoryVertexOutputFormat.getOutputGraph();
        assertEquals(16, output.getVertices().size());
        assertTrue(output.getVertex(new Text("1")).getValue().getApproxBetweenness() >= 0.0);
        assertTrue(output.getVertex(new Text("9")).getValue().getApproxBetweenness() >= 0.0);
        assertEquals(output.getVertex(new Text("2")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("3")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("4")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("5")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("6")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("7")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("8")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("10")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("11")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("12")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("13")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("14")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("15")).getValue().getApproxBetweenness(), 0.0, 0.0);
        assertEquals(output.getVertex(new Text("16")).getValue().getApproxBetweenness(), 0.0, 0.0);

    }

    private TestGraph<Text, VertexData, Text> getTwoCriticalPointGraph(GiraphConfiguration conf) {
        TestGraph<Text, VertexData, Text> testGraph = new TestGraph<Text, VertexData, Text>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("6"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("7"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("8"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("10"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("11"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("12"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("13"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("14"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("15"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("16"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("9"), new Text("1"));
        testGraph.addEdge(new Text("9"), new Text("1"), new Text("1"));
        return testGraph;
    }

    private TestGraph<Text, VertexData, Text> getShortestPathOneTestGraph(GiraphConfiguration conf) {
        TestGraph<Text, VertexData, Text> testGraph = new TestGraph<Text, VertexData, Text>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("3"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("4"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("5"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("6"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("7"), new Text("1"));
        testGraph.addEdge(new Text("1"), new Text("8"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("6"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("7"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("8"), new Text("1"), new Text("1"));
        return testGraph;
    }

    private TestGraph<Text, VertexData, Text> getFirstTestGraph(GiraphConfiguration conf) {
        TestGraph<Text, VertexData, Text> testGraph = new TestGraph<Text, VertexData, Text>(conf);
        testGraph.addEdge(new Text("1"), new Text("2"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("3"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("4"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("5"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("6"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("7"), new Text("1"), new Text("1"));
        testGraph.addEdge(new Text("2"), new Text("8"), new Text("1"));
        return testGraph;
    }
}
