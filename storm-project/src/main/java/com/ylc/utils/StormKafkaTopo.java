package com.ylc.utils;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.trident.state.JdbcState;
import org.apache.storm.jdbc.trident.state.JdbcStateFactory;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.shade.com.google.common.collect.Lists;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.topology.TopologyBuilder;

import java.sql.Types;
import java.util.List;
import java.util.Map;
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

        // 写入数据库
        Map hikariConfigMap = Maps.newHashMap();
        hikariConfigMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm");
        hikariConfigMap.put("dataSource.user","root");
        hikariConfigMap.put("dataSource.password","123456");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);
        String tableName = "stat";
//        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);
        List<Column> columnSchema = Lists.newArrayList(
                new Column("time", Types.BIGINT),
                new Column("lat", Types.DOUBLE),
                new Column("lng", Types.DOUBLE));
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(columnSchema);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);


        builder.setBolt("JdbcInsertBolt",userPersistanceBolt).shuffleGrouping(boltId);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopo.class.getSimpleName(),
                new Config(),
                builder.createTopology());
    }
}
