package com.ylc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
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

import java.util.Map;

/**
 * 累计求和
 */
public class LocalSumStormTopology {
    /**
     * spout
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector spoutOutputCollector;
        private int number = 0;

        @Override
        public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
            this.spoutOutputCollector = spoutOutputCollector;
        }

        @Override
        public void nextTuple() {
            this.spoutOutputCollector.emit(new Values(number++));
            System.out.println("Spout:" + number);
            Utils.sleep(1000);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("num"));
        }
    }

    /**
     * bolt
     */
    public static class SumBolt extends BaseRichBolt {

        private int sum = 0;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        }

        @Override
        public void execute(Tuple tuple) {
            Integer num = tuple.getIntegerByField("num");
            sum += num;
            System.out.println("bolt:" + sum);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SumBolt", new SumBolt()).shuffleGrouping("DataSourceSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalSumStormTopology", new Config(), builder.createTopology());
    }
}
