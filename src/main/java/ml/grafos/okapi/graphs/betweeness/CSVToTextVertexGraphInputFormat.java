package ml.grafos.okapi.graphs.betweeness;

import de.unipassau.fim.dimis.schlegel.types.WritableLabel;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.ReverseEdgeDuplicator;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class CSVToTextVertexGraphInputFormat extends TextEdgeInputFormat<Text, Text> {

    private static final Logger INPUT_FORMAT_LOGGER = LoggerFactory.getLogger(CSVToTextVertexGraphInputFormat.class);
    private static final Logger INPUT_FORMAT_READER_LOGGER = LoggerFactory.getLogger(CSVToDirectedLabelGraphInputFormatReader.class);

    // CSV split regex
    private static final Pattern SEPARATOR = Pattern.compile(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");

    @Override
    public EdgeReader<Text, Text> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {

        EdgeReader<Text, Text> edgeReader = new CSVToDirectedLabelGraphInputFormatReader();
        INPUT_FORMAT_LOGGER.trace("createEdgeReader({}, {}) -> {}", inputSplit, taskAttemptContext, edgeReader);

        return new ReverseEdgeDuplicator<Text, Text>(edgeReader);
    }

    /**
     * The format reader used to convert csv statements to graph labels.
     */
    protected class CSVToDirectedLabelGraphInputFormatReader extends TextEdgeReaderFromEachLineProcessed<String[]> {
        /**
         * Processes the line given by splitting the n-triple statement to at least
         * four strings.
         *
         * @param text the n-triples statement to split
         * @return a string array with the splits
         * @throws IOException
         */
        @Override
        protected String[] preprocessLine(Text text) throws IOException {
            String string = text.toString().trim();
            String[] tokens = CSVToTextVertexGraphInputFormat.SEPARATOR.split(string);
            INPUT_FORMAT_READER_LOGGER.debug("preprocessLine({}) -> {}", text, tokens);
            return tokens;
        }

        /**
         * Returns the subject label from the given statement splits.
         *
         * @param strings the statement splits as returned by preprocessLine
         * @return the subject node of the given statement
         * @throws IOException
         */
        @Override
        protected Text getSourceVertexId(String[] strings) throws IOException {
            Text sourceLabel = this.parseLabel(strings[0]);
            INPUT_FORMAT_READER_LOGGER.trace("getSourceVertexId({}) -> {}", strings, sourceLabel);
            return sourceLabel;
        }

        /**
         * Returns the predicate label from the given statement splits.
         *
         * @param strings the statement splits as returned by preprocessLine
         * @return the predicate node of the given statement
         * @throws IOException
         */
        @Override
        protected Text getValue(String[] strings) throws IOException {
            Text edgeValue = this.parseLabel(strings[2]);
            return edgeValue;
        }

        /**
         * Returns the object label from the given statement splits.
         *
         * @param strings the statement splits as returned by preprocessLine
         * @return the object node of the given statement
         * @throws IOException
         */
        @Override
        protected Text getTargetVertexId(String[] strings) throws IOException {
            Text targetLabel = this.parseLabel(strings[1]);
            INPUT_FORMAT_READER_LOGGER.trace("getTargetVertexId({}) -> {}", strings, targetLabel);
            return targetLabel;
        }

        /**
         * Creates a new {@link WritableLabel} from the string given.
         *
         * @param string the string to parse as {@link WritableLabel}
         * @return the {@link WritableLabel} created from the string given
         */
        private Text parseLabel(String string) {

            // Remove enclosing quotations
            // e.g "http://vocabulary.turnguard.com/opendirectory/ElSaadawi" --> http://vocabulary.turnguard.com/opendirectory/ElSaadawi
            String labelString = string.substring(1, Math.max(string.length(), 1) - 1);

            Text parsedLabel = new Text(labelString);
            INPUT_FORMAT_READER_LOGGER.trace("parseLabel({}) -> {}", string, parsedLabel);
            return parsedLabel;
        }
    }
}

