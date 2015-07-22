
package ml.grafos.okapi.graphs.betweeness;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;


public class BetweenessData implements Writable {

    private static final Logger logger = LoggerFactory.getLogger(BetweenessData.class);
    private Double betweenness;
    private BigInteger numPaths;
    private Map<String, ShortestPathList> pathDataMap;


    public BetweenessData() {
        betweenness = 0.0;
        // because we have a reflexiv shortest path
        numPaths = BigInteger.valueOf(1);
        pathDataMap = new HashMap<String, ShortestPathList>();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        // write betweeness
        out.writeDouble(betweenness);

        // write numpaths
        out.writeLong(numPaths.longValue());

        out.writeInt(pathDataMap.size());

        for (Entry<String, ShortestPathList> entry : pathDataMap.entrySet()) {
            Text.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        //Reset Maps
        pathDataMap.clear();

        // read betweeness
        betweenness = in.readDouble();

        // read numpaths
        numPaths = BigInteger.valueOf(in.readLong());

        int size = in.readInt();

        for (int i = 0; i < size; i++) {
            String key = Text.readString(in);
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


    public Map<String, ShortestPathList> getPathDataMap() {
        return pathDataMap;
    }

    public ShortestPathList addPathData(ShortestPathData data) {
        ShortestPathList list = getPathDataMap().get(data.getSource());

        // if the list was empty ad the first item and return
        if (list == null) {
            list = new ShortestPathList(data);
            getPathDataMap().put(data.getSource(), list);
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

    public void setPathDataMap(Map<String, ShortestPathList> pathDataMap) {
        this.pathDataMap = pathDataMap;
    }

    @Override
    public String toString() {
        return numPaths.toString() + ", " + pathDataMap.size() + ", " + betweenness;
    }
}
