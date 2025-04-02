package com.debopam.retryablemessagelistener.optionone;

import com.rabbitmq.client.ShutdownSignalException;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionListener;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ErrorHandler;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A resilient RabbitMQ message listener container that extends {@link SimpleMessageListenerContainer}
 * with auto-reconnection, fault tolerance, and metrics collection capabilities.
 *
 * <p>Features include:
 * <ul>
 *   <li>Automatic reconnection after network failures</li>
 *   <li>Configurable retry policies with multiple backoff strategies</li>
 *   <li>Dead Letter Queue (DLQ) routing for failed messages</li>
 *   <li>Metrics collection for monitoring</li>
 *   <li>Thread-safe operation</li>
 * </ul>
 */
public class ResilientRabbitListenerContainer extends SimpleMessageListenerContainer {

    /** Maximum number of retry attempts for message processing */
    private int maxRetries = 3;

    /** Initial delay between retry attempts in milliseconds */
    private long retryInterval = 1000;

    /** Multiplier for exponential backoff strategy */
    private double backoffMultiplier = 2.0;

    /** Strategy for calculating retry delays */
    private BackoffStrategy backoffStrategy = BackoffStrategy.EXPONENTIAL;

    /** Custom parameters for backoff strategy (when strategy is CUSTOM) */
    private Map<String, Long> customBackoffParameters;

    // DLQ Configuration

    /** Flag to enable/disable DLQ routing */
    private boolean dlqEnabled = false;

    /** Exchange for dead letter messages */
    private String dlqExchange;

    /** Routing key for dead letter messages */
    private String dlqRoutingKey;

    /** Template for sending messages to DLQ */
    private RabbitTemplate rabbitTemplate;

    //Metrics

    /** Collector for processing metrics */
    private MetricsCollector metricsCollector;

    // Error handling
    private MessageRecoverer errorHandler;
    private ErrorHandler containerErrorHandler;

    /** Counter for successfully processed messages */
    private final AtomicInteger processedMessages = new AtomicInteger(0);

    /** Counter for failed messages */
    private final AtomicInteger failedMessages = new AtomicInteger(0);

    /** Counter for retry attempts */
    private final AtomicInteger retryAttempts = new AtomicInteger(0);

    // Connection listener for auto-reconnect
    private final ConnectionListener connectionListener = new ConnectionListener() {
        @Override
        public void onCreate(Connection connection) {
            // Connection created
        }

        @Override
        public void onClose(Connection connection) {
            // Connection closed, schedule a restart
            scheduleRestart();
        }

        @Override
        public void onShutDown(ShutdownSignalException signal) {
            // Connection shut down, schedule a restart
            scheduleRestart();
        }
    };

    /**
     * Initializes the container with configured settings.
     * @throws IllegalStateException if DLQ is enabled but required configuration is missing
     */
    @Override
    public void afterPropertiesSet() {
        validateConfiguration();
        configureRetryInterceptor();
        configureErrorHandling();
        configureConnectionFactory();
        addConnectionListener();
        super.afterPropertiesSet();
    }

    protected void validateConfiguration() {
        if (dlqEnabled && (rabbitTemplate == null || dlqExchange == null)) {
            throw new IllegalStateException("DLQ requires rabbitTemplate and dlqExchange");
        }
    }

    private void configureRetryInterceptor() {
        RetryTemplate retryTemplate = createRetryTemplate();
        MessageRecoverer recoverer = createMessageRecoverer();

        RetryOperationsInterceptor interceptor = RetryInterceptorBuilder.stateless()
                .retryOperations(retryTemplate)
                .recoverer(recoverer)
                .build();

        setAdviceChain(interceptor);
    }

    private void configureErrorHandling() {
        if (containerErrorHandler == null) {
            containerErrorHandler = new DefaultContainerErrorHandler(metricsCollector);
        }
        setErrorHandler(containerErrorHandler);
    }

    /**
     * Configures the connection factory for auto-recovery and resilience.
     */
    private void configureConnectionFactory() {
        if (getConnectionFactory() instanceof CachingConnectionFactory) {
            CachingConnectionFactory connectionFactory = (CachingConnectionFactory) getConnectionFactory();
            connectionFactory.setPublisherReturns(true);
            connectionFactory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
            connectionFactory.setRequestedHeartBeat(60); // 60 seconds
            connectionFactory.setConnectionTimeout(10000); // 10 seconds
            //connectionFactory.setRecoveryInterval(5000); // 5 seconds
            connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);
        }
    }

    private void addConnectionListener() {
        if (getConnectionFactory() instanceof CachingConnectionFactory) {
            getConnectionFactory().addConnectionListener(connectionListener);
        }
    }

    /**
     * Creates a RetryTemplate with configured policy and backoff strategy.
     * @return configured RetryTemplate instance
     */
    private RetryTemplate createRetryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(new SimpleRetryPolicy(maxRetries + 1)); // +1 for initial attempt
        template.setBackOffPolicy(createBackOffPolicy());
        return template;
    }

    /**
     * Creates the appropriate BackOffPolicy based on configured strategy.
     * @return BackOffPolicy instance
     */
    BackOffPolicy createBackOffPolicy() {
        return switch (backoffStrategy) {
            case FIXED -> new FixedBackOffPolicy() {{
                setBackOffPeriod(retryInterval);
            }};
            case EXPONENTIAL -> new ExponentialBackOffPolicy() {{
                setInitialInterval(retryInterval);
                setMultiplier(backoffMultiplier);
                setMaxInterval(10000L);
            }};
            case CUSTOM -> new CustomBackOffPolicy(customBackoffParameters);
        };
    }

    private MessageRecoverer createMessageRecoverer() {
        return errorHandler != null ? errorHandler : new DlqMessageRecoverer();
    }

    /**
     * Handles exceptions during message processing.
     * @param ex the exception that occurred
     */
    @Override
    protected void handleListenerException(Throwable ex) {
        if (ex instanceof ListenerExecutionFailedException) {
            handleMessageProcessingFailure(ex);
        } else {
            handleInfrastructureFailure(ex);
        }
    }

    /**
     * Handles message processing failures.
     * @param ex the processing exception
     */
    private void handleMessageProcessingFailure(Throwable ex) {
        containerErrorHandler.handleError(ex);
        failedMessages.incrementAndGet();
        logger.warn("Message processing failed", ex);
    }

    /**
     * Handles infrastructure failures and schedules restart.
     * @param ex the infrastructure exception
     */
    private void handleInfrastructureFailure(Throwable ex) {
        super.handleListenerException(ex);
        logger.error("Infrastructure failure detected", ex);
        scheduleRestart();
    }

    /**
     * Schedules container restart with configurable retry behavior.
     * Implements exponential backoff for restart attempts and reports
     * final failure to metrics collector if unable to recover.
     */
    private void scheduleRestart() {
        final int maxRetryAttempts = 5; // Configurable maximum attempts
        final long initialDelayMs = 5000; // Configurable initial delay (5 seconds)
        final double backoffMultiplier = 1.5; // Configurable backoff multiplier

        new Thread(() -> {
            long currentDelay = initialDelayMs;
            int attempt = 0;
            boolean recovered = false;

            while (attempt < maxRetryAttempts && !recovered && !Thread.currentThread().isInterrupted()) {
                attempt++;
                try {
                    // Wait with increasing delay
                    Thread.sleep(currentDelay);

                    logger.info(String.format("Attempting restart (%d/%d)", attempt, maxRetryAttempts));
                    start(); // Try to restart

                    // Verify recovery
                    if (isRunning() && getConnectionFactory().createConnection().isOpen()) {
                        recovered = true;
                        logger.info(String.format("Successfully recovered after %d attempts", attempt));
                        if (metricsCollector != null) {
                            metricsCollector.onRecovery(attempt);
                        }
                    }

                    // Increase delay for next attempt
                    currentDelay = (long)(currentDelay * backoffMultiplier);

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Restart thread interrupted");
                } catch (Exception e) {
                    logger.error(String.format("Restart attempt %s failed", attempt), e);
                }
            }

            // Report permanent failure if not recovered
            if (!recovered && metricsCollector != null) {
                metricsCollector.onRecoveryFailure(maxRetryAttempts);
            }

        }, "RabbitMQ-Restart-Thread").start();
    }

    // Default error handler for container-level failures
    private static class DefaultContainerErrorHandler implements ErrorHandler {
        private final MetricsCollector metricsCollector;

        public DefaultContainerErrorHandler(MetricsCollector metricsCollector) {
            this.metricsCollector = metricsCollector;
        }

        @Override
        public void handleError(Throwable t) {
            if (metricsCollector != null) {
                metricsCollector.onFailure(null, t);
            }
        }
    }

    // DLQ message recoverer
    private class DlqMessageRecoverer implements MessageRecoverer {
        @Override
        public void recover(Message message, Throwable cause) {
            if (metricsCollector != null) {
                metricsCollector.onFailure(message, cause);
            }

            if (dlqEnabled) {
                enhanceMessageHeaders(message, cause);
                rabbitTemplate.send(dlqExchange, dlqRoutingKey, message);
            }
        }

        private void enhanceMessageHeaders(Message message, Throwable cause) {
            MessageProperties props = message.getMessageProperties();
            props.getHeaders().put("x-retry-attempts", maxRetries);
            props.getHeaders().put("x-original-queue", getQueueNames());
            props.getHeaders().put("x-exception-message", cause.getMessage());
        }
    }

    // Metrics collector interface
    public interface MetricsCollector {
        void onFailure(Message message, Throwable cause);

        void onRetry(Message message, int attempt);

        void onSuccess(Message message);

        /**
         * Called when recovery succeeds after failures
         * @param attempts Number of attempts before success
         */
        void onRecovery(int attempts);

        /**
         * Called when all recovery attempts fail
         * @param maxAttempts Maximum attempts that were configured
         */
        void onRecoveryFailure(int maxAttempts);
    }

    // Backoff strategy types
    public enum BackoffStrategy {
        FIXED, EXPONENTIAL, CUSTOM
    }

    // Custom backoff policy
    private static class CustomBackOffPolicy implements BackOffPolicy {
        private final Map<String, Long> parameters;

        public CustomBackOffPolicy(Map<String, Long> parameters) {
            this.parameters = parameters;
        }

        @Override
        public BackOffContext start(RetryContext context) {
            return new CustomBackOffContext(parameters);
        }

        @Override
        public void backOff(BackOffContext backOffContext) {
            CustomBackOffContext context = (CustomBackOffContext) backOffContext;
            try {
                Long interval = parameters.getOrDefault("interval" + context.attempt, 1000L);
                Thread.sleep(interval);
                context.attempt++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        private static class CustomBackOffContext implements BackOffContext {
            int attempt = 0;
            final Map<String, Long> parameters;

            public CustomBackOffContext(Map<String, Long> parameters) {
                this.parameters = parameters;
            }
        }
    }

    // Configuration setters
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public void setBackoffMultiplier(double backoffMultiplier) {
        this.backoffMultiplier = backoffMultiplier;
    }

    public void setBackoffStrategy(BackoffStrategy backoffStrategy) {
        this.backoffStrategy = backoffStrategy;
    }

    public void setCustomBackoffParameters(Map<String, Long> params) {
        this.customBackoffParameters = params;
    }

    public void setDlqEnabled(boolean enabled) {
        this.dlqEnabled = enabled;
    }

    public void setDlqExchange(String exchange) {
        this.dlqExchange = exchange;
    }

    public void setDlqRoutingKey(String routingKey) {
        this.dlqRoutingKey = routingKey;
    }

    public void setRabbitTemplate(RabbitTemplate template) {
        this.rabbitTemplate = template;
    }

    public void setMetricsCollector(MetricsCollector collector) {
        this.metricsCollector = collector;
    }

    public void setContainerErrorHandler(ErrorHandler errorHandler) {
        this.containerErrorHandler = errorHandler;
    }
}
