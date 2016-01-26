package ml.grafos.okapi.graphs;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

/**
 * Created by schlegel on 19/01/16.
 */
public class DegreeCountComputation extends BasicComputation<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void compute(Vertex<IntWritable, IntWritable, IntWritable> vertex, Iterable<IntWritable> messages) throws IOException {
        if (getSuperstep() == 0) {
            sendMessageToAllEdges(vertex, new IntWritable(1));
        } else {
            int sum = 0;
            for (IntWritable message : messages) {
                sum++;
            }

            vertex.setValue(new IntWritable(sum));

            // vertex.setValue(new IntWritable(vertex.getNumEdges()));
            vertex.voteToHalt();
        }
    }
}
