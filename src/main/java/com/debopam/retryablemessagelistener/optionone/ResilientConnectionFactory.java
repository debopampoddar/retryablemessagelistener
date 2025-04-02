package com.debopam.retryablemessagelistener.optionone;

import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.context.annotation.Bean;

public class ResilientConnectionFactory extends CachingConnectionFactory {

    public ResilientConnectionFactory(com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory) {
        super(rabbitConnectionFactory);
        configureResilience();
    }

    private void configureResilience() {
        // Enable automatic connection recovery
        this.setPublisherReturns(true);
        this.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);

        // Connection recovery settings
        this.setConnectionTimeout(10000); // 10 seconds
        this.setRequestedHeartBeat(60); // 60 seconds
        //this.setRecoveryInterval(5000); // 5 seconds
        this.setConnectionCacheSize(10); // Pool size
    }

    @Bean
    public static ConnectionFactory resilientConnectionFactory() {
        com.rabbitmq.client.ConnectionFactory rabbitConnectionFactory = new com.rabbitmq.client.ConnectionFactory();
        rabbitConnectionFactory.setAutomaticRecoveryEnabled(true);
        rabbitConnectionFactory.setNetworkRecoveryInterval(5000); // 5 seconds
        rabbitConnectionFactory.setRequestedHeartbeat(60); // 60 seconds

        return new ResilientConnectionFactory(rabbitConnectionFactory);
    }
}
