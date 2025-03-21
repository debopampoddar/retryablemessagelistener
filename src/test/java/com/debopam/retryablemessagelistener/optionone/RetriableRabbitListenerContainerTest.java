package com.debopam.retryablemessagelistener.optionone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.util.ErrorHandler;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetriableRabbitListenerContainerTest {

    @Mock Message message;
    @Mock RabbitTemplate rabbitTemplate;
    @Mock
    RetriableRabbitListenerContainer.MetricsCollector metricsCollector;
    @Mock ErrorHandler errorHandler;
    @InjectMocks RetriableRabbitListenerContainer container;

    @Test
    void testContainerRestartOnInfrastructureFailure() {
        container.setContainerErrorHandler(errorHandler);
        container.start();

        // Simulate infrastructure failure
        container.handleListenerException(new RuntimeException("Connection failed"));

        verify(errorHandler).handleError(any());
        assertTrue(container.isRunning()); // Container should restart
    }

    @Test
    void testMessageProcessingFailure() {
        container.setMetricsCollector(metricsCollector);
        container.setDlqEnabled(true);
        container.setDlqExchange("dlx");
        container.setDlqRoutingKey("dlq");
        container.setRabbitTemplate(rabbitTemplate);

        MessageProperties props = new MessageProperties();
        when(message.getMessageProperties()).thenReturn(props);

        // Simulate message processing failure
        container.handleListenerException(new ListenerExecutionFailedException("Processing failed", new RuntimeException()));

        verify(metricsCollector).onFailure(any(), any());
        verify(rabbitTemplate).send(eq("dlx"), eq("dlq"), any(Message.class));
    }

    @Test
    void testCustomBackoffStrategy() {
        container.setBackoffStrategy(BackoffStrategy.CUSTOM);
        container.setCustomBackoffParameters(Map.of("interval1", 500L));

        BackOffPolicy policy = container.createBackOffPolicy();
        assertInstanceOf(CustomBackOffPolicy.class, policy);
    }

    @Test
    void testFixedBackoffStrategy() {
        container.setBackoffStrategy(RetriableRabbitListenerContainer.BackoffStrategy.FIXED);
        container.setRetryInterval(2000);

        BackOffPolicy policy = container.createBackOffPolicy();
        assertInstanceOf(FixedBackOffPolicy.class, policy);
        assertEquals(2000, ((FixedBackOffPolicy) policy).getBackOffPeriod());
    }

    @Test
    void testExponentialBackoffStrategy() {
        container.setBackoffStrategy(RetriableRabbitListenerContainer.BackoffStrategy.EXPONENTIAL);
        container.setRetryInterval(1000);
        container.setBackoffMultiplier(2.0);

        BackOffPolicy policy = container.createBackOffPolicy();
        assertInstanceOf(ExponentialBackOffPolicy.class, policy);
        assertEquals(1000, ((ExponentialBackOffPolicy) policy).getInitialInterval());
        assertEquals(2.0, ((ExponentialBackOffPolicy) policy).getMultiplier(), 0.01);
    }
}
