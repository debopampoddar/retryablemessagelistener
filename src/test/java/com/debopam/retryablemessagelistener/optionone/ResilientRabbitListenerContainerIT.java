package com.debopam.retryablemessagelistener.optionone;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = RabbitMQTestConfig.class)
@Testcontainers
@Import(RabbitMQTestConfig.class)
public class ResilientRabbitListenerContainerIT {

    private final int MAX_MESSAGES = 50;

    @Container
    static RabbitMQContainer rabbitMQ = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.12-management")
    ).withExposedPorts(5672, 15672)
            .withPluginsEnabled("rabbitmq_management")
            .withEnv("RABBITMQ_DEFAULT_USER", "guest")
            .withEnv("RABBITMQ_DEFAULT_PASS", "guest")
            .withReuse(true);

    @Autowired
    private CachingConnectionFactory connectionFactory;

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private ResilientRabbitListenerContainer container;

    private static final String QUEUE_NAME = "test.queue";
    private final AtomicInteger processedMessages = new AtomicInteger(0);

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbitMQ::getHost);
        registry.add("spring.rabbitmq.port", rabbitMQ::getAmqpPort);
        registry.add("spring.rabbitmq.username", rabbitMQ::getAdminUsername);
        registry.add("spring.rabbitmq.password", rabbitMQ::getAdminPassword);
    }

    @Test
    void testAutoReconnectAndMessageProcessing() throws Exception {
        // Setup queue and listener
        rabbitTemplate.execute(channel -> {
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.queuePurge(QUEUE_NAME);
            return null;
        });

        // Use a latch to control message processing
        CountDownLatch processingLatch = new CountDownLatch(2);
        container.setMessageListener((Message message) -> {
            try {
                Thread.sleep(Duration.ofSeconds(1).toMillis());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            System.out.println("Processing message: " + new String(message.getBody()));
            processedMessages.incrementAndGet();
            processingLatch.countDown();
        });
        container.setQueueNames(QUEUE_NAME);
        container.start();

        // Send 4 test messages
        for (int i = 0; i < MAX_MESSAGES; i++) {
            rabbitTemplate.convertAndSend(QUEUE_NAME, "Message " + (i + 1));
        }

        // Wait for first 2 messages to be processed
        assertTrue(processingLatch.await(10, TimeUnit.SECONDS),
                "First 2 messages should be processed");

        // Simulate network failure by closing connection
        connectionFactory.resetConnection();

        // Verify connection recovery and remaining message processing
        await()
                .atMost(MAX_MESSAGES + 5, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    // Verify all messages are eventually processed
                    assertTrue(processedMessages.get() >= MAX_MESSAGES,
                            "All messages should be processed after reconnection");

                    // Verify connection is alive
                    assertTrue(connectionFactory.createConnection().isOpen(),
                            "Connection should be re-established");
                });
    }
}
