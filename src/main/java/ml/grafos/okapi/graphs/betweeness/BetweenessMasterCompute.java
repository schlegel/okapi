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


import ml.grafos.okapi.graphs.hbse.HBSEMasterCompute;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BetweenessMasterCompute extends DefaultMasterCompute {

    private static final Logger logger = LoggerFactory.getLogger(HBSEMasterCompute.class);

    /**
     * Aggregator Identifier that gets the state of the computation.
     */
    public static final String STATE_AGG = "com.sotera.graph.singbetweenness.STATE_AGG";

    /**
     * Aggregator Identifier for the number of nodes changed in the highbetweenness list comparison.
     */
    public static final String UPDATE_COUNT_AGG = "com.sotera.graph.singbetweenness.UPDATE_COUNT_AGG";


    /**
     * Variable that tracks the current state of computation.
     */
    private State state = State.START;

    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        state = State.START;
        this.registerPersistentAggregator(STATE_AGG, IntOverwriteAggregator.class);
        this.registerAggregator(UPDATE_COUNT_AGG, IntSumAggregator.class);
    }

    /**
     * Gets a Require Configuration value.
     *
     * @param name The configuration name to get.
     * @return A valid integer
     * @throws NumberFormatException if it can't be parsed to an int.
     */
    private int getRequiredHBSEConfiguration(String name) {
        String propValue = getConf().get(name);
        try {
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            logger.error("Option not set or invalid. 'name' must be set to a valid int, was set to: {}", name, propValue);
            throw e;
        }
    }

    /**
     * Gets an optional configuration value and only logs a NumberFormatException.
     *
     * @param name         The configuration name to get.
     * @param defaultValue The default value to be returned if there is an error.
     * @return Integer Configuration Value
     */
    private int getOptionalHBSEConfiguration(String name, int defaultValue) {
        String propValue = getConf().get(name);
        try {
            if (propValue == null)
                return defaultValue;
            return Integer.parseInt(propValue);
        } catch (NumberFormatException e) {
            logger.error("Option not set or invalid. {} must be set to a valid int, was set to: {}", name, defaultValue);
            return defaultValue;
        }
    }

    /**
     * Coordinates the computation phases of SBVertex by monitoring for the completion of each state
     * and moving to the next state.
     * <ol>
     * <li>selects pivots</li>
     * <li>monitors for completion of shortest paths</li>
     * <li>starts pair dependency phase</li>
     * <li>monitors for completion of pair dependency</li>
     * <li>checks high betweenness set stability</li>
     * <li>if set is stable save set and exit else select new pivots and start new shortest path phase</li>
     * </ol>
     */
    @Override
    public void compute() {
        long step = this.getSuperstep();
        int updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
        logger.info("Superstep: {} starting in State: {}", step, state);
        switch (state) {
            case START:
                state = State.SHORTEST_PATH_START;
                setGlobalState(state);
                logger.info("Superstep: {} Switched to State: {}", step, state);
                break;
            case SHORTEST_PATH_START:
                logger.info("Superstep: {} Paths updated: {}", step, updateCount);
                state = State.SHORTEST_PATH_RUN;
                setGlobalState(state);
                logger.info("Superstep: {} Switched to State: {}", step, state);
                break;
            case SHORTEST_PATH_RUN:
                logger.info("Superstep: {} Paths updated: {}", step, updateCount);
                if (updateCount == 0) {
                    state = State.BETWEENESSS_PING;
                    setGlobalState(state);
                    logger.info("Superstep: {} UPDATE COUNT 0, Switched to State: {}", step, state);
                } else {
                    state = State.SHORTEST_PATH_RUN;
                    setGlobalState(state);
                    logger.info("Superstep: {} Stay in State: {} Updated: {}", step, state, updateCount);
                }
                break;
            case BETWEENESSS_PING:
                state = State.BETWEENESSS_POPULATE;
                setGlobalState(state);
                logger.info("Superstep: {} UPDATE COUNT 0, Switched to State: {}", step, state);
                break;
            case BETWEENESSS_POPULATE:
                if (updateCount == 0) {
                    state = State.BETWEENESSS_CALCULATE;
                    setGlobalState(state);
                    logger.info("Superstep: {} UPDATE COUNT 0, Switched to State: {}", step, state);
                } else {
                    state = State.BETWEENESSS_POPULATE;
                    setGlobalState(state);
                    logger.info("Superstep: {} Stay in State: {} Updated: {}", step, state, updateCount);
                }
                break;
            case BETWEENESSS_CALCULATE:
                state = State.FINISHED;
                setGlobalState(state);
                logger.info("Superstep: {} UPDATE COUNT 0, Switched to State: {}", step, state);
                break;

//            case PAIR_DEPENDENCY_PING_PREDECESSOR:
//                state = State.PAIR_DEPENDENCY_FIND_SUCCESSORS;
//                setGlobalState(state);
//                logger.info("Superstep: {} Switched to State: {}", step, state);
//                break;
//            case PAIR_DEPENDENCY_FIND_SUCCESSORS:
//                state = State.PAIR_DEPENDENCY_RUN;
//                setGlobalState(state);
//                logger.info("Superstep: {} Switched to State: {}", step, state);
//                break;
//            case PAIR_DEPENDENCY_RUN:
//                updateCount = ((IntWritable) this.getAggregatedValue(UPDATE_COUNT_AGG)).get();
//                if (updateCount == 0) {
//                    state = State.PAIR_DEPENDENCY_COMPLETE;
//                    setGlobalState(state);
//                }
//                logger.info("Superstep: {} UPDATE COUNT: {} STATE: {}", step, updateCount, state);
//                break;
            case FINISHED:
                logger.info("Superstep: {} Aggregated NUMPATHS {}", step, updateCount);
                this.haltComputation();
                break;
            default:
                logger.error("INVALID STATE: {}", state);
                throw new IllegalStateException("Invalid State" + state);
        }
    }

    /**
     * Set the value of the state aggregator
     *
     * @param state The current state of computation.
     */
    private void setGlobalState(State state) {
        this.setAggregatedValue(STATE_AGG, new IntWritable(state.ordinal()));
    }

}
