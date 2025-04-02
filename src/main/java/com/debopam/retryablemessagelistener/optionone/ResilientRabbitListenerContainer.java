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

public class ResilientRabbitListenerContainer extends SimpleMessageListenerContainer {

    // Retry configuration
    private int maxRetries = 3;
    private long retryInterval = 1000;
    private double backoffMultiplier = 2.0;
    private BackoffStrategy backoffStrategy = BackoffStrategy.EXPONENTIAL;
    private Map<String, Long> customBackoffParameters;

    // DLQ configuration
    private boolean dlqEnabled = false;
    private String dlqExchange;
    private String dlqRoutingKey;
    private RabbitTemplate rabbitTemplate;

    // Metrics
    private MetricsCollector metricsCollector;

    // Error handling
    private MessageRecoverer errorHandler;
    private ErrorHandler containerErrorHandler;

    // Thread-safe counters for metrics
    private final AtomicInteger processedMessages = new AtomicInteger(0);
    private final AtomicInteger failedMessages = new AtomicInteger(0);
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
            ((CachingConnectionFactory) getConnectionFactory()).addConnectionListener(connectionListener);
        }
    }

    private RetryTemplate createRetryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(new SimpleRetryPolicy(maxRetries + 1)); // +1 for initial attempt
        template.setBackOffPolicy(createBackOffPolicy());
        return template;
    }

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

    @Override
    protected void handleListenerException(Throwable ex) {
        if (ex instanceof ListenerExecutionFailedException) {
            // Handle message processing failures
            containerErrorHandler.handleError(ex);
            failedMessages.incrementAndGet();
        } else {
            // Handle infrastructure failures
            super.handleListenerException(ex);
            scheduleRestart();
        }
    }

    private void scheduleRestart() {
        new Thread(() -> {
            try {
                Thread.sleep(5000); // Wait 5 seconds before restarting
                start();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }).start();
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
