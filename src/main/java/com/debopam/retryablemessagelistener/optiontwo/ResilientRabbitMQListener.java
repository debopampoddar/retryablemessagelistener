package com.debopam.retryablemessagelistener.optiontwo;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * A resilient RabbitMQ listener that extends {@link SimpleMessageListenerContainer} to provide
 * automatic reconnection, retry mechanisms, and metrics collection.
 */
@Component
public class ResilientRabbitMQListener extends SimpleMessageListenerContainer {

    private final Map<String, MessageProcessor> processorMap;
    private final MeterRegistry meterRegistry;
    private final RetryTemplate retryTemplate;

    /**
     * Creates a RabbitMQ connection factory with automatic reconnection enabled.
     *
     * @return the configured connection factory
     */
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

    /**
     * Creates a new instance of {@link ResilientRabbitMQListener}.
     *
     * @param connectionFactory the RabbitMQ connection factory
     * @param processorMap      a map of message processors for different queues
     * @param meterRegistry     the meter registry for metrics collection
     */
    public ResilientRabbitMQListener(ConnectionFactory connectionFactory,
                                     Map<String, MessageProcessor> processorMap,
                                     MeterRegistry meterRegistry) {
        super(connectionFactory);
        this.processorMap = processorMap;
        this.meterRegistry = meterRegistry;
        this.retryTemplate = createRetryTemplate();

        // Set the queue names to listen to
        //this.setQueueNames("queue1", "queue2");
        this.setAcknowledgeMode(AcknowledgeMode.AUTO);
        this.setConcurrentConsumers(2);
        this.setMaxConcurrentConsumers(5);

        // Set the message listener with retry logic
        this.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
            String queueName = message.getMessageProperties().getConsumerQueue();
            String messageBody = new String(message.getBody());

            // Start a timer for retry metrics
            Timer.Sample retryTimer = Timer.start(meterRegistry);

            try {
                // Execute the message processing with retry
                retryTemplate.execute(context -> {
                    processMessage(queueName, messageBody);
                    return null;
                });
            } catch (Exception e) {
                System.err.println("Final failure after retries. Waiting for connection recovery.");
            } finally {
                // Stop the retry timer
                retryTimer.stop(meterRegistry.timer("rabbitmq.retry.time"));
            }
        });
    }

    /**
     * Processes a message from the specified queue.
     *
     * @param queueName the name of the queue
     * @param message   the message to process
     */
    void processMessage(String queueName, String message) {
        // Start a timer for processing metrics
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
            // Stop the processing timer
            timerSample.stop(meterRegistry.timer("rabbitmq.processing.time"));
        }
    }

    /**
     * Creates a {@link RetryTemplate} with a simple retry policy and exponential backoff policy.
     *
     * @return the configured retry template
     */
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