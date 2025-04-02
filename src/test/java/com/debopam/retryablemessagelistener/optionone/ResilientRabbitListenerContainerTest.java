package com.debopam.retryablemessagelistener.optionone;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.ListenerExecutionFailedException;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.util.ErrorHandler;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ResilientRabbitListenerContainerTest {

    @Mock
    Message message;
    @Mock
    RabbitTemplate rabbitTemplate;
    @Mock
    ResilientRabbitListenerContainer.MetricsCollector metricsCollector;
    @Mock
    ErrorHandler errorHandler;
    @InjectMocks
    ResilientRabbitListenerContainer container;

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
        container.setBackoffStrategy(ResilientRabbitListenerContainer.BackoffStrategy.CUSTOM);
        container.setCustomBackoffParameters(Map.of("interval1", 500L));

        BackOffPolicy policy = container.createBackOffPolicy();
        assertNotNull(policy, "BackOffPolicy should not be null");
        //assertInstanceOf(CustomBackOffPolicy.class, policy);
    }

    @Test
    void testFixedBackoffStrategy() {
        container.setBackoffStrategy(ResilientRabbitListenerContainer.BackoffStrategy.FIXED);
        container.setRetryInterval(2000);

        BackOffPolicy policy = container.createBackOffPolicy();
        assertInstanceOf(FixedBackOffPolicy.class, policy);
        assertEquals(2000, ((FixedBackOffPolicy) policy).getBackOffPeriod());
    }

    @Test
    void testExponentialBackoffStrategy() {
        container.setBackoffStrategy(ResilientRabbitListenerContainer.BackoffStrategy.EXPONENTIAL);
        container.setRetryInterval(1000);
        container.setBackoffMultiplier(2.0);

        BackOffPolicy policy = container.createBackOffPolicy();
        assertInstanceOf(ExponentialBackOffPolicy.class, policy);
        assertEquals(1000, ((ExponentialBackOffPolicy) policy).getInitialInterval());
        assertEquals(2.0, ((ExponentialBackOffPolicy) policy).getMultiplier(), 0.01);
    }

    @Test
    void testRestartWithMultipleAttempts() throws Exception {
        // Configure shorter retry settings for test
        //container.setRecoverySettings(3, 1000, 1.5);

        // Setup mock metrics collector
        ResilientRabbitListenerContainer.MetricsCollector mockCollector = mock(ResilientRabbitListenerContainer.MetricsCollector.class);
        container.setMetricsCollector(mockCollector);

        // Force connection failure
        ((CachingConnectionFactory) container.getConnectionFactory()).destroy();

        // Verify restart attempts
        verify(mockCollector, timeout(10000).atLeastOnce()).onRecovery(anyInt());

        // OR if all attempts fail
        // verify(mockCollector, timeout(15000)).onRecoveryFailure(3);
    }
}
