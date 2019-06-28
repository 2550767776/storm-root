package com.ylc.utils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

import java.util.regex.Pattern;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST;

/**
 * kafka连接到storm
 */
public class StormKafkaTopo {

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        String port = 9092 + "";

        String spoutId = KafkaSpout.class.getSimpleName();
        builder.setSpout(spoutId, new KafkaSpout<>(
                KafkaSpoutConfig.builder("127.0.0.1:" + port, Pattern.compile("project.*")).setFirstPollOffsetStrategy(LATEST).build()), 1);

        String boltId = LogProcessBolt.class.getSimpleName();
        builder.setBolt(boltId,new LogProcessBolt()).shuffleGrouping(spoutId);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(),
                new Config(),
                builder.createTopology());
    }
}
