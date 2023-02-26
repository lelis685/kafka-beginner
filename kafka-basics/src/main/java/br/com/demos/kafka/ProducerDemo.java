package br.com.demos.kafka;


import br.com.demos.kafka.config.ClusterProperties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class ProducerDemo {

    private static ClusterProperties clusterProperties;


    @Autowired
    public void setClusterProperties(ClusterProperties clusterProperties) {
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

        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "batata world " + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            System.out.println("Partition: " + metadata.partition()  + " Key: " + key );
                        } else {
                            System.err.println("Error while producing " + e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }


        producer.flush();

        producer.close();

    }

}
