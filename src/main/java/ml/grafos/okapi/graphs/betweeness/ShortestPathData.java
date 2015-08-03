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
import java.util.LinkedList;
import java.util.List;

public class ShortestPathData implements Writable {

    /**
     * The Distance of the path.
     */
    private int distance;

    /**
     * The source node of the path.
     */
    private String source;

    /**
     * the predecessor OR successor node (for shortest path OR pair betweeness ping).
     */
    private String from;

    private List<String> shortestPathSources;


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
        distance = Integer.MAX_VALUE;
        source = "-1";
        from = "-1";
        shortestPathSources = new LinkedList<>();
    }

    /**
     * Get a new PathData message for shortest path computation.
     *
     * @param source   The source Id
     * @param from     The predecessor Id.
     * @param distance The distance from the source to the predecessor.
     * @return a New PathData Object
     */
    public static ShortestPathData createShortestPathMessage(String source, String from, int distance) {
        ShortestPathData data = new ShortestPathData();
        data.setSource(source);
        data.setFrom(from);
        data.setDistance(distance);
        return data;
    }

    public static ShortestPathData getPingMessage(List<String> sources, String from) {
        ShortestPathData data = new ShortestPathData();
        data.setShortestPathSources(sources);
        data.setFrom(from);
        data.setDistance(0);
        return data;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, source);
        Text.writeString(out, from);
        out.writeInt(distance);

        if(shortestPathSources != null) {
            out.writeInt(shortestPathSources.size());
        } else {
            out.writeInt(0);
        }


        for(String source : shortestPathSources) {
            Text.writeString(out, source);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        source = Text.readString(in);
        from = Text.readString(in);
        distance = in.readInt();

        int size = in.readInt();
        shortestPathSources = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            shortestPathSources.add(Text.readString(in));
        }
    }

    /**
     * Gets the distance from source to a predecessor.
     *
     * @return The distance value.
     */
    public int getDistance() {
        return distance;
    }

    /**
     * Sets the distance from a source to a predecessor.
     *
     * @param distance Distance value to set it to.
     */
    public void setDistance(int distance) {
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

    public void setShortestPathSources(List<String> shortestPathSources) {
        this.shortestPathSources = shortestPathSources;
    }

    public List<String> getShortestPathSources() {
        return shortestPathSources;
    }
}
