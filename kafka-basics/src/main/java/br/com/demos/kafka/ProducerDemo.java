package br.com.demos.kafka;


import br.com.demos.kafka.config.ClusterProperties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

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

        producer.send(producerRecord);

        producer.flush();

        producer.close();

    }

}
