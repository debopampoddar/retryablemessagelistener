package com.debopam.retryablemessagelistener.optionone;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQTestConfig {

    @Value("${spring.rabbitmq.host}")
    private String rabbitHost;

    @Value("${spring.rabbitmq.port}")
    private int rabbitPort;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Bean
    public CachingConnectionFactory connectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory();
        factory.setHost(rabbitHost);
        factory.setPort(rabbitPort);
        factory.setUsername(username);
        factory.setPassword(password);

        // Configure for auto-recovery
        factory.setRequestedHeartBeat(60); // 60 seconds
        factory.setConnectionTimeout(10000); // 10 seconds
        //factory.setRecoveryInterval(5000); // 5 seconds
        //factory.setAutomaticRecoveryEnabled(true);

        return factory;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(CachingConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @Bean
    public ResilientRabbitListenerContainer resilientRabbitListenerContainer(
            CachingConnectionFactory connectionFactory) {
        ResilientRabbitListenerContainer container = new ResilientRabbitListenerContainer();
        container.setConnectionFactory(connectionFactory);
        return container;
    }
}
