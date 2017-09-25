package com.xysy.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by cdduanjianfeng on 2017/9/25.
 */
public class SentenceSpout extends BaseRichSpout {

    private Logger logger = LoggerFactory.getLogger(SentenceSpout.class);
    private SpoutOutputCollector collector;

    private static final String[] words = {"I have a dream", "my dog is ugly", "you don't like to eat", "I like to eat dog"};

    private int index = 0;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    public synchronized void nextTuple() {
        if(index <words.length){
            Values values = new Values(words[index++]);
            this.collector.emit(values);
            logger.info("send tuple:{}",values.toString());
        }else{
            index = 0;
        }
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
