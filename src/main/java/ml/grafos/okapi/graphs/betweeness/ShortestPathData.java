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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;


/**
 * Message that can be passed between vertices.  Used for both shortest path computation and pair dependency
 * accumulation.  Some fields are only used in one of the two phases, or take on different meaning in the different phases.
 * <p/>
 * For more information on this method of calculation betweenness centrality see
 * "U. Brandes, A Faster Algorithm for Betweenness Centrality"
 */
public class ShortestPathData implements Writable {

    /**
     * The Distance of the path.
     */
    private BigInteger distance;

    /**
     * The source node of the path.
     */
    private String source;

    /**
     * the predecessor OR successor node (for shortest path OR pair betweeness ping).
     */
    private String from;


    /**
     * The Default Constructor for PathData:
     * <ul>
     * <li>Sets Distance to the Max long value.</li>
     * <li>Sets source to -1.</li>
     * <li>Sets from to -1.</li>
     * <li>Sets dependency to -1.</li>
     * <li>Sets numPaths to -1.</li>
     * </ul>
     */
    public ShortestPathData() {
        distance = BigInteger.valueOf(Long.MAX_VALUE);
        source = "-1";
        from = "-1";
    }

    /**
     * Get a new PathData message for shortest path computation.
     *
     * @param source   The source Id
     * @param from     The predecessor Id.
     * @param distance The distance from the source to the predecessor.
     * @return a New PathData Object
     */
    public static ShortestPathData createShortestPathMessage(String source, String from, BigInteger distance) {
        ShortestPathData data = new ShortestPathData();
        data.setSource(source);
        data.setFrom(from);
        data.setDistance(distance);
        return data;
    }

    /**
     * Get a new PathData message for sending successor / predecessor information to neighbors
     *
     * @param source The source that the ping came from.
     * @return A new PathData message for Ping purposes.
     */
    public static ShortestPathData getPingMessage(String source, String from) {
        ShortestPathData data = new ShortestPathData();
        data.setSource(source);
        data.setFrom(from);
        return data;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, source);
        Text.writeString(out, from);
        Text.writeString(out, distance.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {

        source = Text.readString(in);
        from = Text.readString(in);

        distance = new BigInteger(Text.readString(in));
    }

    /**
     * Gets the distance from source to a predecessor.
     *
     * @return The distance value.
     */
    public BigInteger getDistance() {
        return distance;
    }

    /**
     * Sets the distance from a source to a predecessor.
     *
     * @param distance Distance value to set it to.
     */
    public void setDistance(BigInteger distance) {
        this.distance = distance;
    }

    /**
     * Gets the source value.
     *
     * @return The source value.
     */
    public String getSource() {
        return source;
    }

    /**
     * Sets the source value.
     *
     * @param source The value to set the source to.
     */
    public void setSource(String source) {
        this.source = source;
    }

    /**
     * Gets the predecessor/successor value.
     *
     * @return The value of from.
     */
    public String getFrom() {
        return from;
    }

    /**
     * Sets the value of from.
     *
     * @param from The value to set from to.
     */
    public void setFrom(String from) {
        this.from = from;
    }
}
