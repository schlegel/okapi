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

import com.google.common.base.Joiner;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Keeps Shortest path data for a single source vertex to a single target vertex.
 * <p/>
 * Maintains Shortest path, predecessors, and number of shortest paths from the source
 * to each predecessor.
 */
public class ShortestPathList implements Writable {

    /**
     * The distance from source to this vertex on the shortest path.
     */
    private int distance;

    /**
     * The map of predecessor to number of shortest paths from source to that predecessor
     */
    private Set<Integer> predecessors;

    /**
     * Create a new shortest empty Path List
     */
    public ShortestPathList() {
        distance = Integer.MAX_VALUE;
        setPredecessors(new HashSet<Integer>());
    }

    /**
     * Create a new shortest path list based on a
     * shortest path message.
     *
     * @param data Path data to add to the map.
     */
    public ShortestPathList(ShortestPathData data) {
        this();
        this.distance = data.getDistance();
        predecessors.add(data.getFrom());
    }


    /**
     * Update This shortest path list based on a new shortest path message
     *
     * @param data A new path data message.
     * @return true if the ShortestPathList is modified in anyway, otherwise false.
     */
    public boolean update(ShortestPathData data) {
        if (data.getDistance() == this.distance) {
            if (!this.predecessors.contains(data.getFrom())) {
                predecessors.add(data.getFrom());
                return true;
            } else {
                return false;
            }
        } else if (data.getDistance() < this.distance) {
            this.distance = data.getDistance();
            this.predecessors.clear();
            predecessors.add(data.getFrom());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(distance);
        out.writeInt(this.predecessors.size());
        for (Integer entry : predecessors) {
            out.writeInt(entry);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.predecessors.clear();
        setDistance(in.readInt());
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            predecessors.add(in.readInt());
        }
    }

    /**
     * @return The distance from a source to this vertex.
     */
    public int getDistance() {
        return distance;
    }

    /**
     * Sets the distance from a source to this vertex.
     *
     * @param distance The distance to set it to.
     */
    public void setDistance(int distance) {
        this.distance = distance;
    }

    public Set<Integer> getPredecessors() {
        return predecessors;
    }

    public void setPredecessors(Set<Integer> predecessors) {
        this.predecessors = predecessors;
    }

    @Override
    public String toString() {
        Joiner joiner = Joiner.on(',');

        return " distance=" + distance  + "predecessors= " + joiner.join(predecessors);
    }
}