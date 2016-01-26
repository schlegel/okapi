package ml.grafos.okapi.graphs.betweeness;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class CSVToIntVertexDoubleEdge extends TextEdgeInputFormat<IntWritable, DoubleWritable> {

    private static final Logger INPUT_FORMAT_LOGGER = LoggerFactory.getLogger(CSVToTextVertexGraphInputFormatNoReverse.class);
    // CSV split regex
    //private static final Pattern SEPARATOR = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
    private static final Pattern SEPARATOR = Pattern.compile(",");

    @Override
    public EdgeReader<IntWritable, DoubleWritable> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {

        EdgeReader<IntWritable, DoubleWritable> edgeReader = new IntIDReader();
        INPUT_FORMAT_LOGGER.trace("createEdgeReader({}, {}) -> {}", inputSplit, taskAttemptContext, edgeReader);

        return edgeReader;
    }

    protected class IntIDReader extends TextEdgeReaderFromEachLineProcessed<String[]> {

        @Override
        protected String[] preprocessLine(Text line) throws IOException {
            return SEPARATOR.split(line.toString());
        }

        @Override
        protected IntWritable getSourceVertexId(String[] tokens)
                throws IOException {
            return new IntWritable(Integer.parseInt(tokens[0]));
        }

        @Override
        protected IntWritable getTargetVertexId(String[] tokens)
                throws IOException {
            return new IntWritable(Integer.parseInt(tokens[1]));
        }

        @Override
        protected DoubleWritable getValue(String tokens[]) throws IOException {
            return new DoubleWritable(0);
        }
    }
}

