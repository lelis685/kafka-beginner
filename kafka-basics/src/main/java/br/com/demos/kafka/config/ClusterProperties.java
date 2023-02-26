package br.com.demos.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

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

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }
}
