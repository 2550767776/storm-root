package com.ylc;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * 词频统计
 */
public class LocalWordCountTopology {
    /**
     * spout
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector spoutOutputCollector;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            Collection<File> files = FileUtils.listFiles(new File("/home/ylc/Desktop"), new String[]{"txt"}, true);
            for (File file : files) {
                try {
                    List<String> strings = FileUtils.readLines(file);
                    for (String string : strings) {
                        spoutOutputCollector.emit(new Values(string));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            Utils.sleep(100000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line"));
        }
    }

    /**
     * bolt
     */
    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector outputCollector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getStringByField("line");
            String[] strings = line.split(" ");
            for (String string : strings) {
                outputCollector.emit(new Values(string));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }
    }

    /**
     * bolt
     */
    public static class CountBolt extends BaseRichBolt {
        private Map<String,Integer> map = new HashMap<>();
        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        @Override
        public void execute(Tuple tuple) {
            String word = tuple.getStringByField("word");
            if (map.containsKey(word)) {
                map.put(word,map.get(word)+1);
            } else {
                map.put(word,1);
            }

            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            for (Map.Entry<String, Integer> entry : entries) {
                System.out.println(entry);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("splitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("countBolt", new CountBolt()).shuffleGrouping("splitBolt");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountTopology", new Config(), builder.createTopology());
    }
}
