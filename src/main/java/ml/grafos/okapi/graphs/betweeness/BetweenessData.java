
package ml.grafos.okapi.graphs.betweeness;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class BetweenessData implements Writable {
    private double betweenness;
    private long closeness;
    private double avgShortestPathDistance;
    private BigInteger numPaths;
    private Map<Integer, ShortestPathList> pathDataMap;

    public BetweenessData() {
        // because we have a reflexiv shortest path
        numPaths = BigInteger.valueOf(1);
        pathDataMap = new HashMap<Integer, ShortestPathList>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // write betweeness
        out.writeDouble(betweenness);
        out.writeLong(closeness);
        out.writeDouble(avgShortestPathDistance);

        // write numpaths
        out.writeLong(numPaths.longValue());

        out.writeInt(pathDataMap.size());

        for (Entry<Integer, ShortestPathList> entry : pathDataMap.entrySet()) {
            out.writeInt(entry.getKey());
            entry.getValue().write(out);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //Reset Maps
        pathDataMap.clear();

        // read betweeness
        betweenness = in.readDouble();
        closeness = in.readLong();
        avgShortestPathDistance = in.readDouble();

        // read numpaths
        numPaths = BigInteger.valueOf(in.readLong());

        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            Integer key= in.readInt();
            ShortestPathList list = new ShortestPathList();
            list.readFields(in);
            pathDataMap.put(key, list);
        }
    }

    public double getBetweenness() {
        return betweenness;
    }

    public void setBetweenness(double value) {
        this.betweenness = value;
    }

    public BigInteger getNumPaths() {
        return numPaths;
    }

    public void setNumPaths(BigInteger numPaths) {
        this.numPaths = numPaths;
    }

    public void addNumberofPaths(BigInteger value) {
        this.numPaths = this.numPaths.add(value);
    }


    public Map<Integer, ShortestPathList> getPathDataMap() {
        return pathDataMap;
    }

    public ShortestPathList addPathData(ShortestPathData data) {
        ShortestPathList list = pathDataMap.get(data.getSource());

        // if the list was empty ad the first item and return
        if (list == null) {
            list = new ShortestPathList(data);
            pathDataMap.put(data.getSource(), list);
            return list;
        } else {
            boolean result = list.update(data);

            if(result) {
                return list;
            } else {
                return null;
            }
        }

    }

    public long getCloseness() {
        return closeness;
    }

    public void setCloseness(long closeness) {
        this.closeness = closeness;
    }

    public void setPathDataMap(Map<Integer, ShortestPathList> pathDataMap) {
        this.pathDataMap = pathDataMap;
    }

    @Override
    public String toString() {
        return numPaths.toString() + ", " + pathDataMap.size() + ", " + betweenness;
    }

    public void setAvgShortestPathDistance(double avgShortestPathDistance) {
        this.avgShortestPathDistance = avgShortestPathDistance;
    }

    public double getAvgShortestPathDistance() {
        return avgShortestPathDistance;
    }
}
