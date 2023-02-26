package br.com.demos.kafka;


import br.com.demos.kafka.config.ClusterProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class ProducerDemo {

    private static ClusterProperties clusterProperties;


    @Autowired
    public void setClusterProperties(ClusterProperties clusterProperties){
        ProducerDemo.clusterProperties = clusterProperties;
    }

    public static void main(String[] args) {

        SpringApplication.run(ProducerDemo.class, args);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", clusterProperties.getBootstrapServers());
        properties.setProperty("security.protocol", clusterProperties.getSecurityProtocol());
        properties.setProperty("sasl.jaas.config", clusterProperties.getSaslJaasConfig());
        properties.setProperty("sasl.mechanism", clusterProperties.getSaslMechanism());

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null){
                    System.out.println("Received new metadata \n"+
                            "Topic: " + metadata.topic() + "\n"+
                            "Partition: " + metadata.partition() + "\n"+
                            "Offset: " + metadata.offset() + "\n"+
                            "Timestamp: " + metadata.timestamp() + "\n"
                    );
                }else{
                    System.err.println("Error while producing " + e);
                }
            }
        });

        producer.flush();

        producer.close();

    }

}
