package com.xysy;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.xysy.bolt.wordcount.CountBolt;
import com.xysy.bolt.wordcount.SplitBolt;
import com.xysy.spout.SentenceSpout;

/**
 * Created by cdduanjianfeng on 2017/9/25.
 */
public class WordCount {
    private static final String SENTENCE_SPOUT_ID = "sentenceSpout";

    private static final String SPLIT_BOLT_ID = "splitBolt";

    private static final String COUNT_BOLT_ID = "countBolt";

    private static final String TOPOLOGY_NAME = "wordCount";

    public static void main(String[] args) {

        int workerNum = 1;
        int executorNum = 2;
        int numTask = 2;

        Config config = new Config();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, new SentenceSpout(), 1);
        topologyBuilder.setBolt(SPLIT_BOLT_ID, new SplitBolt(), executorNum).setNumTasks(numTask).localOrShuffleGrouping(SENTENCE_SPOUT_ID);
        topologyBuilder.setBolt(COUNT_BOLT_ID, new CountBolt(), executorNum).setNumTasks(numTask).globalGrouping(SPLIT_BOLT_ID);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        localCluster.killTopology(TOPOLOGY_NAME);
        localCluster.shutdown();
    }
}
