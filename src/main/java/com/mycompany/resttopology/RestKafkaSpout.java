/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.resttopology;

import java.util.UUID;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 *
 * @author shivaraj
 */
public class RestKafkaSpout {
    public static void main(String[] args) throws Exception{
       
      Config config = new Config();
      config.setDebug(true);
      config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
      String zkConnString = "192.168.56.102:2080,192.168.56.103:2080,192.168.56.105:2080";
      String topic = "rest-topic";
      BrokerHosts hosts = new ZkHosts(zkConnString);
      
      SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,    
         UUID.randomUUID().toString());
      kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
      kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
//      kafkaSpoutConfig.forceFromStart = true;
      kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
      builder.setBolt("json-des", new DJsonBolt()).shuffleGrouping("kafka-spout");
   //   builder.setBolt("word-counter", new CountBolt()).shuffleGrouping("word-spitter");
  //    builder.setBolt("forwardToKafka", new WriteToKafka()).shuffleGrouping("word-counter");   
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("RestKafkaSpout", config, builder.createTopology());

      Thread.sleep(100000);
      cluster.shutdown();
}

}
