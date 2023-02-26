package br.com.demos.kafka;


import br.com.demos.kafka.config.ClusterProperties;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@SpringBootApplication
public class ConsumerShutDownDemo {

    private static ClusterProperties clusterProperties;


    @Autowired
    public void setClusterProperties(ClusterProperties clusterProperties) {
        ConsumerShutDownDemo.clusterProperties = clusterProperties;
    }

    public static void main(String[] args) {
        SpringApplication.run(ConsumerShutDownDemo.class, args);


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

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Detected a shutdown, calling consumer.wakeup()");
                consumer.wakeup();

                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });


        try {
            consumer.subscribe(List.of(topic));

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (var record : records) {
                    System.out.println("Key: " + record.key());
                    System.out.println("value: " + record.value());
                    System.out.println("partition: " + record.partition());
                    System.out.println("offset: " + record.offset());
                }
            }
        }catch (WakeupException e){
            System.out.println("consumer starting shut down");
        }catch (Exception e){
            System.out.println("Uncaught Error " + e );
        }finally {
            consumer.close();
            System.out.println("consumer is shut down");
        }


    }

}
