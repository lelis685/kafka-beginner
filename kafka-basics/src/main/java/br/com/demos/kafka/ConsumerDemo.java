package br.com.demos.kafka;


import br.com.demos.kafka.config.ClusterProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class ConsumerDemo {

    private static ClusterProperties clusterProperties;


    @Autowired
    public void setClusterProperties(ClusterProperties clusterProperties) {
        ConsumerDemo.clusterProperties = clusterProperties;
    }

    public static void main(String[] args) {
        SpringApplication.run(ConsumerDemo.class, args);


        String groupId = "my-java-app";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", clusterProperties.getBootstrapServers());
        properties.setProperty("security.protocol", clusterProperties.getSecurityProtocol());
        properties.setProperty("sasl.jaas.config", clusterProperties.getSaslJaasConfig());
        properties.setProperty("sasl.mechanism", clusterProperties.getSaslMechanism());

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

       while(true){
           System.out.println("Polling");

           ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

           for (var record : records){
               System.out.println("Key: " + record.key());
               System.out.println("value: " + record.value());
               System.out.println("partition: " + record.partition());
               System.out.println("offset: " + record.offset());
           }
       }


    }

}
