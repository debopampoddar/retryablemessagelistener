package com.debopam.retryablemessagelistener.optiontwo;

import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;

@Component
public class ResilientRabbitMQListener extends SimpleMessageListenerContainer {

    private final Map<String, MessageProcessor> processorMap;
    private final MeterRegistry meterRegistry;
    private final RetryTemplate retryTemplate;

    private ConnectionFactory createRabbitMQConnectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost");
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // Enable automatic reconnection
        connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);
        connectionFactory.getRabbitConnectionFactory().setNetworkRecoveryInterval(5000);
        connectionFactory.getRabbitConnectionFactory().setRequestedHeartbeat(30);

        return connectionFactory;
    }

    public ResilientRabbitMQListener(ConnectionFactory connectionFactory,
                                     Map<String, MessageProcessor> processorMap,
                                     MeterRegistry meterRegistry) {
        super(connectionFactory);
        this.processorMap = processorMap;
        this.meterRegistry = meterRegistry;
        this.retryTemplate = createRetryTemplate();

        this.setQueueNames("queue1", "queue2");
        this.setAcknowledgeMode(AcknowledgeMode.AUTO);
        this.setConcurrentConsumers(2);
        this.setMaxConcurrentConsumers(5);

        this.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            String queueName = message.getMessageProperties().getConsumerQueue();
            String messageBody = new String(message.getBody());

            Timer.Sample retryTimer = Timer.start(meterRegistry);

            try {
                retryTemplate.execute(context -> {
                    processMessage(queueName, messageBody);
                    return null;
                });
            } catch (Exception e) {
                System.err.println("Final failure after retries. Waiting for connection recovery.");
            } finally {
                retryTimer.stop(meterRegistry.timer("rabbitmq.retry.time"));
            }
        });
    }

    void processMessage(String queueName, String message) {
        Timer.Sample timerSample = Timer.start(meterRegistry);
        try {
            MessageProcessor processor = processorMap.get(queueName);
            if (processor != null) {
                processor.processMessage(message);
                meterRegistry.counter("rabbitmq.messages.processed").increment();
            } else {
                System.out.println("No processor found for queue: " + queueName);
            }
        } finally {
            timerSample.stop(meterRegistry.timer("rabbitmq.processing.time"));
        }
    }

    private RetryTemplate createRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // Simple retry policy - Retry up to 10 times
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy(10);

        // Exponential backoff - Start with 1s, double on each failure, max 60s
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);
        backOffPolicy.setMultiplier(2.0);
        backOffPolicy.setMaxInterval(60000);

        retryTemplate.setRetryPolicy(retryPolicy);
        retryTemplate.setBackOffPolicy(backOffPolicy);

        return retryTemplate;
    }
}

