package br.com.demos.kafka.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Getter
@Configuration
public class ClusterProperties {

    @Value("${bootstrap.servers}")
    private String bootstrapServers;

    @Value("${security.protocol}")
    private String securityProtocol;

    @Value("${sasl.jaas.config}")
    private String saslJaasConfig;

    @Value("${sasl.mechanism}")
    private String saslMechanism;

}
