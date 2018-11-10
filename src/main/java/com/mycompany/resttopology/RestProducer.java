package com.mycompany.resttopology;

/**
 *
 * @author shivaraj
 */
import java.util.Properties;
import java.util.Scanner;

import java.net.HttpURLConnection;
import java.net.URL;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RestProducer {

    public static void main(String[] args) {

        String topicName = "rest-topic";

        // create instance for properties to access producer configs and set properties  
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.102:9092,192.168.56.103:9092,192.168.56.105:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        try {
            URL url = new URL("https://jsonplaceholder.typicode.com/todos");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }
            Scanner sc = new Scanner(url.openStream());
            String inline = "";
            while (sc.hasNext()) {

                inline += sc.nextLine();

            }
            producer.send(new ProducerRecord<String, String>(topicName, inline));

            sc.close();

            conn.disconnect();
            System.out.println("Message sent successfully");
            RestKafkaSpout rkf = new RestKafkaSpout();
            producer.close();

        } catch (Exception e) {
            System.out.println(e);
        }

    }
}
