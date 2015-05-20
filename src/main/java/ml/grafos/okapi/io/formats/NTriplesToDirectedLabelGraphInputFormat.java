package ml.grafos.okapi.io.formats;

import de.unipassau.fim.dimis.schlegel.types.WritableLabel;
import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Pattern;

public class NTriplesToDirectedLabelGraphInputFormat extends TextEdgeInputFormat<WritableLabel, DoubleWritable> {

    private static final Logger INPUT_FORMAT_LOGGER =
            LoggerFactory.getLogger(NTriplesToDirectedLabelGraphInputFormat.class);
    private static final Logger INPUT_FORMAT_READER_LOGGER =
            LoggerFactory.getLogger(NTriplesToDirectedLabelGraphInputFormatReader.class);

    // The pattern will match any sequence of space or tab characters that are
    // followed by either < (opening bracket), " (quotation mark), or . (dot).
    // This allows whitespace in string literals. The following ntriples line
    // can be split into four strings using the given pattern:
    //
    // <http://example.org/show/218> <http://www.w3.org/2000/01/rdf-schema#label>
    //      "That Seventies Show"^^<http://www.w3.org/2001/XMLSchema#string> .
    //
    // with the resulting string splits being
    // 1: <http://example.org/show/218>
    // 2: <http://www.w3.org/2000/01/rdf-schema#label>
    // 3: "That Seventies Show"^^<http://www.w3.org/2001/XMLSchema#string>
    // 4: .
    //
    // Note that the third sequence's whitespace is not subject to a split, as
    // it is part of the string literal.
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]+(?=[<\".])");

    /**
     * Returns a new edge reader used to convert n-triple statements to their
     * corresponding labels in the RDF directed graph representation.
     *
     * @param inputSplit         the input split to run the edge reader on
     * @param taskAttemptContext any context information
     * @return a new edge reader instance
     * @throws IOException
     */
    @Override
    public EdgeReader<WritableLabel, DoubleWritable> createEdgeReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {

        EdgeReader<WritableLabel, DoubleWritable> edgeReader = new NTriplesToDirectedLabelGraphInputFormatReader();
        INPUT_FORMAT_LOGGER.debug("createEdgeReader({}, {}) -> {}", inputSplit, taskAttemptContext, edgeReader);
        return edgeReader;
    }

    /**
     * The format reader used to convert n-triple statements to graph labels.
     */
    protected class NTriplesToDirectedLabelGraphInputFormatReader extends TextEdgeReaderFromEachLineProcessed<String[]> {

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
            String[] tokens = NTriplesToDirectedLabelGraphInputFormat.SEPARATOR.split(string);
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
        protected WritableLabel getSourceVertexId(String[] strings) throws IOException {
            WritableLabel sourceLabel = this.parseLabel(strings[0]);
            INPUT_FORMAT_READER_LOGGER.debug("getSourceVertexId({}) -> {}", strings, sourceLabel);
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
        protected DoubleWritable getValue(String[] strings) throws IOException {
//            WritableLabel edgeValue = this.parseLabel(strings[1]);
//            INPUT_FORMAT_READER_LOGGER.debug("getValue({}) -> {}", strings, edgeValue);
//            return edgeValue;
            return new DoubleWritable(0.0);
        }

        /**
         * Returns the object label from the given statement splits.
         *
         * @param strings the statement splits as returned by preprocessLine
         * @return the object node of the given statement
         * @throws IOException
         */
        @Override
        protected WritableLabel getTargetVertexId(String[] strings) throws IOException {

            String string = strings[2];
            int lastIndex = Math.max(string.length(), 1) - 1;

            // N-triples does not dictate any whitespace before the statement delimiter.
            // Remove the delimiter if it is part of the object split.
            if (string.charAt(lastIndex) == '.') {
                string = string.substring(0, lastIndex);
            }

            WritableLabel targetLabel = this.parseLabel(string);
            INPUT_FORMAT_READER_LOGGER.debug("getTargetVertexId({}) -> {}", strings, targetLabel);
            return targetLabel;
        }

        /**
         * Creates a new {@link WritableLabel} from the string given.
         *
         * @param string the string to parse as {@link WritableLabel}
         * @return the {@link WritableLabel} created from the string given
         */
        private WritableLabel parseLabel(String string) {

            // Remove enclosing bracket chars '<' and '>'.
            String labelString = string.substring(1, Math.max(string.length(), 1) - 1);

            WritableLabel parsedLabel = new WritableLabel(labelString);
            INPUT_FORMAT_READER_LOGGER.debug("parseLabel({}) -> {}", string, parsedLabel);
            return parsedLabel;
        }
    }
}
