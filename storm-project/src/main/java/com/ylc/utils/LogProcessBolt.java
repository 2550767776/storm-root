package com.ylc.utils;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class LogProcessBolt extends BaseRichBolt {

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
//        byte[] bytes = tuple.getBinaryByField("default");
//        byte[] bytes = tuple.getBinaryByField("bytes");
//        String string = new String(bytes);
//        System.out.println(string+"*********");

        // 13677777777	116.38631,39.837209	2019-06-28 20:48:53
        String value = (String) tuple.getValues().get(4);
        String[] values = value.split("\t");
        String phone = values[0];
        String[] address = values[1].split(",");
        String lng = address[0];
        String lat = address[1];
        try {
            Long time = format.parse(values[2]).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
