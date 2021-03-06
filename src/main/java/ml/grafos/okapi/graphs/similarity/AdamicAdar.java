/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs.similarity;

import de.unipassau.fim.dimis.schlegel.types.WritableLabel;
import ml.grafos.okapi.common.data.LabelArrayListWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * This class computes the Adamic-Adar similarity or distance
 * for each pair of neighbors in an undirected unweighted graph.  
 * 
 * To get the exact Adamic-Adar similarity, run the command:
 * 
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.AdamicAdar\$ComputeLogOfInverseDegree  \
 *   -mc  ml.grafos.okapi.graphs.AdamicAdar\$MasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.oneToAllMsgSending=true \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges \
 *   -ca adamicadar.approximation.enabled=false
 * </pre>
 * 
 * Use -ca distance.conversion.enabled=true to get the Adamic-Adar distance instead.
 * 
 * To get the approximate Adamic-Adar similarity 
 * set the adamicadar.approximation.enabled parameter to true.
 *
 */
public class AdamicAdar {

  /** Enables the approximation computation */
  public static final String ADAMICADAR_APPROXIMATION = "adamicadar.approximation.enabled";
  
  /** Default value for approximate computation */
  public static final boolean ADAMICADAR_APPROXIMATION_DEFAULT = false;

  /** Size of bloom filter in bits */
  public static final String BLOOM_FILTER_BITS = "adamicadar.bloom.filter.bits";
  
  /** Default size of bloom filters */
  public static final int BLOOM_FILTER_BITS_DEFAULT = 16;
  
  /** Number of functions to use in bloom filter */
  public static final String BLOOM_FILTER_FUNCTIONS = "adamicadar.bloom.filter.functions";
  
  /** Default number of functions to use in bloom filter */
  public static final int BLOOM_FILTER_FUNCTIONS_DEFAULT = 1;
  
  /** Type of hash function to use in bloom filter */
  public static final String BLOOM_FILTER_HASH_TYPE = "adamicadar.bloom.filter.hash.type";

  /** Default type of hash function in bloom filter */
  public static final int BLOOM_FILTER_HASH_TYPE_DEFAULT = Hash.MURMUR_HASH;
  
  /** Enables the conversion to distance conversion */
  public static final String DISTANCE_CONVERSION = 
      "distance.conversion.enabled";
  
  /** Default value for distance conversion */
  public static final boolean DISTANCE_CONVERSION_DEFAULT = false;

  /**
   * Implements the first step in the Adamic-Adar similarity computation.
   * Each vertex computes the log of its inverse degree and sets this value
   * as its own vertex value. 
   *
   */
  public static class ComputeLogOfInverseDegree extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdDoubleValueFriendsList> {

	@Override
	public void compute( Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex, Iterable<LabelIdDoubleValueFriendsList> messages) throws IOException {
      DoubleWritable vertexValue = new DoubleWritable(0.0);
		if (vertex.getNumEdges() > 0) {
			vertexValue.set(Math.log(1.0 / (double) vertex.getNumEdges()));
		}
		vertex.setValue(vertexValue);
	}
	  
  }

  /**
   * Implements the first step in the exact Adamic-Adar similarity algorithm. 
   * Each vertex broadcasts the list with the IDs of all its neighbors and
   * its own value.
   *
   */
  public static class SendFriendsListAndValue extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdDoubleValueFriendsList> {

	@Override
	public void compute(Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex, Iterable<LabelIdDoubleValueFriendsList> messages) throws IOException {

        LabelArrayListWritable friends = new LabelArrayListWritable();
		
			for (Edge<WritableLabel, DoubleWritable> edge : vertex.getEdges()) {
			      friends.add(WritableUtils.clone(edge.getTargetVertexId(), getConf()));
			}
			
			if (!(friends.isEmpty())) {
              LabelIdDoubleValueFriendsList msg = new LabelIdDoubleValueFriendsList(vertex.getValue(), friends);
			    sendMessageToAllEdges(vertex, msg);
			}
		}
  }

  /**
   * This is the message sent in the implementation of the exact Adamic-Adar
   * similarity. The message contains the source vertex value and a list of vertex
   * ids representing the neighbors of the source.
 
   *
   */
  public static class LabelIdDoubleValueFriendsList implements Writable {

	  private DoubleWritable vertexValue;
	  private LabelArrayListWritable neighbors;
	
	  public LabelIdDoubleValueFriendsList() {
		  this.vertexValue = new DoubleWritable();
		  this.neighbors = new LabelArrayListWritable();
	  }
	  
	  public LabelIdDoubleValueFriendsList(DoubleWritable value, LabelArrayListWritable neighborList) {
		this.vertexValue = value;
		this.neighbors = neighborList;
	}
	  
	  public DoubleWritable getVertexValue() {
		  return this.vertexValue;
	  }
	  
	  public LabelArrayListWritable getNeighborsList() {
		  return this.neighbors;
	  }
	  
	@Override
	public void readFields(DataInput in) throws IOException {
		vertexValue.readFields(in);
		neighbors.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		vertexValue.write(out);
		neighbors.write(out);
		
	}
	  
  }
  
  

  /**
   * Implements the computation of the exact AdamicAdar vertex similarity. The 
   * vertex AdamicAdar similarity between u and v is the sum over the common neighbors
   * of u and v, of the log of the inverse degree of each of them
   * 
   * This computes similarity only between vertices that are connected with 
   * edges, not any pair of vertices in the graph.
   *
   */
  public static class AdamicAdarComputation extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdDoubleValueFriendsList> {

	  boolean conversionEnabled;
	  
	  @Override
	  public void preSuperstep() {
		  conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
	  }

    @Override
    public void compute(Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex, Iterable<LabelIdDoubleValueFriendsList> messages) throws IOException {
      for (LabelIdDoubleValueFriendsList msg : messages) {
        DoubleWritable partialValue = msg.getVertexValue();
        for (WritableLabel id : msg.getNeighborsList()) {
        	if (!id.getLabel().equals(vertex.getId().getLabel())) {
        		if (vertex.getEdgeValue(id) != null) {
        			DoubleWritable currentEdgeValue = vertex.getEdgeValue(id);
        			// if the edge exists, add up the partial value to the current sum
        			vertex.setEdgeValue(id, new DoubleWritable(currentEdgeValue.get() + partialValue.get()));
        		}
        	}	 
        }
      }
      if (!conversionEnabled) {
    	  vertex.voteToHalt();
      }
    }
  }

  public static class ScaleToDistance extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdDoubleValueFriendsList> {

	@Override
	public void compute(
			Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex,
			Iterable<LabelIdDoubleValueFriendsList> messages) throws IOException {
		
		for (Edge<WritableLabel, DoubleWritable> e: vertex.getEdges()) {
			vertex.setEdgeValue(e.getTargetVertexId(), new DoubleWritable(e.getValue().get()*(-1.0)));
		}
		vertex.voteToHalt();
	}
  }
  
  /**
   * This class implements the first computation step in the approximate
   * AdamicAdar similarity algorithm. 
   * A vertex creates a bloom filter and adds the
   * IDs of its neighbors and broadcasts it to all its neighbors,
   * along with its own ID and its value (log of its inverse degree).
   *
   */
  public static class SendFriendsListAndValueBloomFilter extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdAndValueBloomFilter> {

    int numBits;
    int numFunctions;
    int hashType;

    @Override
    public void preSuperstep() {
      numBits = getConf().getInt(BLOOM_FILTER_BITS, BLOOM_FILTER_BITS_DEFAULT);
      numFunctions = getConf().getInt(BLOOM_FILTER_FUNCTIONS, BLOOM_FILTER_FUNCTIONS_DEFAULT);
      hashType = getConf().getInt(BLOOM_FILTER_HASH_TYPE, BLOOM_FILTER_HASH_TYPE_DEFAULT);
    }

    @Override
    public void compute(Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex, Iterable<LabelIdAndValueBloomFilter> messages) throws IOException {
      
      BloomFilter filter = new BloomFilter(numBits, numFunctions, hashType);

      for (Edge<WritableLabel, DoubleWritable> e : vertex.getEdges()) {
        filter.add(new Key(e.getTargetVertexId().getBytes()));
      }

      sendMessageToAllEdges(vertex, new LabelIdAndValueBloomFilter(vertex.getValue(), filter));
    }
  }

  /**
   * 
   * This is the message sent in the approximate Adamic-Adar similiarity 
   * implementation. In this implementation, the message carries (i) the value
   * of the sender of the message, (ii) the bloom filter containing vertex IDs
   *
   */
  public static class LabelIdAndValueBloomFilter implements Writable {

    private DoubleWritable vertexValue;
    private BloomFilter filter;

    public LabelIdAndValueBloomFilter(DoubleWritable value, BloomFilter msg) {
      this.vertexValue = value;
      this.filter = msg;
    }
    
    public DoubleWritable getVertexValue() {
      return vertexValue;
    }
    
    public BloomFilter getNeighborsList() {
    	return filter;
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
      vertexValue.write(output);
      filter.write(output);
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
      vertexValue.readFields(input);
      filter.readFields(input);
    }
  }

  /**
   * 
   * Implements an approximation of the Adamic-Adar vertex similarity. In this
   * implementation, a vertex does not broadcast its entire neighbor list, but
   * a compact summarization of it in the form of a bloom filter. 
   * When this summarization is received, the destination vertices sum-up
   * the partial values from each common neighbor.
   * Due to the possibility of false positives, 
   * vertices may overestimate the number of common neighbors.
   *
   */
  public static class AdamicAdarApproximation extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdAndValueBloomFilter> {

	  boolean conversionEnabled;
	  
	  @Override
	  public void preSuperstep() {
		  conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
	  }

    @Override
    public void compute(Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex, Iterable<LabelIdAndValueBloomFilter> messages) throws IOException {
    	
    	for (LabelIdAndValueBloomFilter msg : messages) {
            DoubleWritable partialValue = msg.getVertexValue();
            BloomFilter filter = msg.getNeighborsList();
            for (Edge<WritableLabel, DoubleWritable> e : vertex.getEdges()) {
            	Key k = new Key(e.getTargetVertexId().getBytes());
                if (filter.membershipTest(k)) { // common neighbor
            		DoubleWritable currentEdgeValue = vertex.getEdgeValue(e.getTargetVertexId());
            		// add up the partial value to the current sum
        			vertex.setEdgeValue(e.getTargetVertexId(), new DoubleWritable (currentEdgeValue.get() 
        					+ partialValue.get()));
            	}	 
            }
          }
    	if (!conversionEnabled) {
    		vertex.voteToHalt();
    	}
       }
  }
  
  public static class ScaleToDistanceBloom extends BasicComputation<WritableLabel, DoubleWritable, DoubleWritable, LabelIdAndValueBloomFilter> {

	@Override
	public void compute(
			Vertex<WritableLabel, DoubleWritable, DoubleWritable> vertex,
			Iterable<LabelIdAndValueBloomFilter> messages) throws IOException {
		
		for (Edge<WritableLabel, DoubleWritable> e: vertex.getEdges()) {
			vertex.setEdgeValue(e.getTargetVertexId(),new DoubleWritable(e.getValue().get()*(-1.0)));
		}
		vertex.voteToHalt();
	}
  }


  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute extends DefaultMasterCompute {

    boolean approximationEnabled;
    boolean conversionEnabled;

    @Override
    public final void initialize() throws InstantiationException,
        IllegalAccessException {
      approximationEnabled = getConf().getBoolean(
          ADAMICADAR_APPROXIMATION, ADAMICADAR_APPROXIMATION_DEFAULT);
      conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
    }

    @Override
    public final void compute() {
    	long superstep = getSuperstep();
	      if (superstep == 0) {
	    	  setComputation(ComputeLogOfInverseDegree.class);
	      }
	      else {
		      if (approximationEnabled) {
		        if (superstep == 1) {
		          setComputation(SendFriendsListAndValueBloomFilter.class);
		        } else if (superstep == 2) {
		          setComputation(AdamicAdarApproximation.class);
		        } else {
		        	if (conversionEnabled) {
		        		setComputation(ScaleToDistanceBloom.class);
		        	}
		        }
		      } else {
		        if (superstep == 1) {
		          setComputation(SendFriendsListAndValue.class);
		        } else if (superstep == 2){
		          setComputation(AdamicAdarComputation.class);
		        } else {
		        	if (conversionEnabled) {
		        		setComputation(ScaleToDistance.class);
		        	}
		        }
		  }
      }
    }
    
  }
}