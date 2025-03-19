package com.debopam.retryablemessagelistener.optionone;

import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.config.RetryInterceptorBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.retry.MessageRecoverer;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.BackOffContext;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Map;

public class RetriableRabbitListenerContainer extends SimpleMessageListenerContainer {

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

    @Override
    public void afterPropertiesSet() {
        validateConfiguration();
        configureRetryInterceptor();
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

    private RetryTemplate createRetryTemplate() {
        RetryTemplate template = new RetryTemplate();
        template.setRetryPolicy(new SimpleRetryPolicy(maxRetries + 1));
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
        if (errorHandler != null) return errorHandler;
        return new DlqMessageRecoverer();
    }

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
        void onRetry(Message message, int attemptCount);
        void onSuccess(Message message);
        void onFailure(Message message, Throwable cause);
    }

    // Backoff strategy types
    public enum BackoffStrategy {
        FIXED, EXPONENTIAL, CUSTOM
    }

    // Custom backoff policy implementation
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
    public void setMaxRetries(int maxRetries) { this.maxRetries = maxRetries; }
    public void setRetryInterval(long retryInterval) { this.retryInterval = retryInterval; }
    public void setBackoffMultiplier(double backoffMultiplier) { this.backoffMultiplier = backoffMultiplier; }
    public void setBackoffStrategy(BackoffStrategy backoffStrategy) { this.backoffStrategy = backoffStrategy; }
    public void setCustomBackoffParameters(Map<String, Long> params) { this.customBackoffParameters = params; }
    public void setDlqEnabled(boolean enabled) { this.dlqEnabled = enabled; }
    public void setDlqExchange(String exchange) { this.dlqExchange = exchange; }
    public void setDlqRoutingKey(String routingKey) { this.dlqRoutingKey = routingKey; }
    public void setRabbitTemplate(RabbitTemplate template) { this.rabbitTemplate = template; }
    public void setMetricsCollector(MetricsCollector collector) { this.metricsCollector = collector; }
}
