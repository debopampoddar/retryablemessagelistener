package com.debopam.retryablemessagelistener.optiontwo;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.AmqpException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;

import static org.mockito.Mockito.*;

class ResilientRabbitMQListenerTest {

    private ResilientRabbitMQListener listener;
    private MessageProcessor mockProcessor;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        mockProcessor = mock(MessageProcessor.class);
        meterRegistry = new SimpleMeterRegistry();

        listener = new ResilientRabbitMQListener(
                connectionFactory,
                Map.of("queue1", mockProcessor),
                meterRegistry
        );
    }

    @Test
    void testProcessMessage_Success() {
        doNothing().when(mockProcessor).processMessage(anyString());
        listener.processMessage("queue1", "test-message");

        verify(mockProcessor, times(1)).processMessage("test-message");
        assert meterRegistry.get("rabbitmq.messages.processed").counter().count() == 1;
    }

    @Test
    void testProcessMessage_WithRetries() {
        doThrow(new RuntimeException("Processing error")).when(mockProcessor).processMessage(anyString());
        listener.processMessage("queue1", "test-message");

        verify(mockProcessor, atMost(10)).processMessage(anyString());
        assert meterRegistry.get("rabbitmq.retry.time").timer().count() > 0;
    }

    @Test
    void testProcessMessage_NoProcessor() {
        listener.processMessage("queue2", "test-message");
        assert meterRegistry.get("rabbitmq.messages.processed").counter().count() == 0;
    }

    @Test
    void testConnectionFailure_Reconnect() {
        doThrow(new AmqpException("Connection lost")).when(mockProcessor).processMessage(anyString());

        listener.processMessage("queue1", "test-message");

        verify(mockProcessor, atMost(10)).processMessage(anyString());
        assert meterRegistry.get("rabbitmq.retry.time").timer().count() > 0;
    }
}

