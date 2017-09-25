package com.xysy.bolt.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.storm.guava.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by cdduanjianfeng on 2017/9/25.
 */
public class CountBolt extends BaseBasicBolt {
    private Logger logger = LoggerFactory.getLogger(CountBolt.class);

    private static final Map<String,Long> countMap = Maps.newConcurrentMap();
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        Long count = countMap.get(word);
        if(count==null){
            countMap.put(word,1l);
        }else{
            countMap.put(word,count+1);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        //该方法不可靠，仅测试用
        for(Map.Entry<String,Long> entry:countMap.entrySet()){
            logger.info("word:{},count:{}",entry.getKey(),entry.getValue());
        }
    }
}
