package com.debopam.retryablemessagelistener.optiontwo;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.FileSystemUtils;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for RabbitMQ using Testcontainers.
 * Ensures the RabbitMQ listener can recover and process messages after failure.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
public class RabbitMQIntegrationTest {
    private static final String QUEUE_NAME = "testQueue";
    private static final String EXCHANGE_NAME = "testExchange";
    private static final String ROUTING_KEY = "testKey";
    private static final int NUM_MESSAGES = 500;

    static Path persistentDataPath = Path.of(System.getProperty("java.io.tmpdir"), "rabbitmq-data");

    /**
     * Testcontainer for RabbitMQ with persistent volume binding.
     */
    private static final RabbitMQContainer rabbitMQContainer =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.0.4-management"))
                    //.withFileSystemBind(persistentDataPath.toAbsolutePath().toString(), "/var/lib/rabbitmq/mnesia")
                    .withExposedPorts(5672, 15672)
                    .withPluginsEnabled("rabbitmq_management")
                    .withEnv("RABBITMQ_DEFAULT_USER", "guest")
                    .withEnv("RABBITMQ_DEFAULT_PASS", "guest")
                    .withReuse(true);

    private CachingConnectionFactory connectionFactory;
    private RabbitAdmin admin;
    private SimpleMessageListenerContainer container;
    private final AtomicReference<String> receivedMessage = new AtomicReference<>("");
    private CountDownLatch latch;

    /**
     * Dynamically sets RabbitMQ connection properties for Spring.
     */
    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbitMQContainer::getHost);
        registry.add("spring.rabbitmq.port", () -> rabbitMQContainer.getMappedPort(5672));
    }

    /**
     * Starts the RabbitMQ container before all tests.
     */
    @BeforeAll
    static void startContainer() {
        System.out.println("RabbitMQ path: " + persistentDataPath);
        try {
            FileSystemUtils.deleteRecursively(persistentDataPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        rabbitMQContainer.start();
        logRabbitMQDetails();
    }

    /**
     * Sets up the test environment before each test.
     */
    @BeforeEach
    void setUp() {
        connectionFactory = createRabbitMQConnectionFactory();
        admin = new RabbitAdmin(connectionFactory);
        declareQueueAndExchange();
        setupMessageListener();
        latch = new CountDownLatch(NUM_MESSAGES);
    }

    /**
     * Tests whether the RabbitMQ listener recovers and processes messages after a simulated failure.
     */
    @Test
    void testListenerRecoversAndProcessesMessageAfterFailure() throws Exception {
        sendMessages(NUM_MESSAGES);
        System.out.println("Sent messages. Queue depth: " + getQueueDepth());

        stopRabbitMQ();
        waitFor(10);  // Simulated downtime
        startRabbitMQ();
        waitUntilRabbitMQStabilizes();

        System.out.println("After restart - Queue depth: " + getQueueDepth());

        boolean messageReceived = latch.await(10, TimeUnit.MINUTES);
        assertTrue(messageReceived, "Listener should recover and process messages");
        assertTrue(receivedMessage.get().startsWith("Persistent Message :"),
                "Listener should correctly process persistent messages");
    }

    /**
     * Sends multiple messages to the RabbitMQ queue.
     */
    private void sendMessages(int count) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        for (int i = 0; i < count; i++) {
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, ROUTING_KEY, "Persistent Message :" + i);
        }
    }

    /**
     * Configures the message listener.
     */
    private void setupMessageListener() {
        container = new ResilientRabbitMQListener(connectionFactory, Map.of(
                QUEUE_NAME, message -> {
                    try {
                        Thread.sleep(Duration.ofMillis(100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    System.out.println("Received message: " + message);
                    receivedMessage.set(message);
                    latch.countDown();
                }
        ), new SimpleMeterRegistry());
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames("testQueue");
        container.start();
    }

    /**
     * Declares RabbitMQ queue and exchange.
     */
    private void declareQueueAndExchange() {
        Queue queue = new Queue(QUEUE_NAME, true);
        admin.declareQueue(queue);
        System.out.println("Deleted #message :"+ admin.purgeQueue(QUEUE_NAME)+" messages from queue :" + QUEUE_NAME);
        DirectExchange exchange = new DirectExchange(EXCHANGE_NAME);
        admin.declareExchange(exchange);
        admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));
    }

    /**
     * Stops RabbitMQ container.
     */
    private void stopRabbitMQ() throws Exception {
        System.out.println("Stopping RabbitMQ...");
        rabbitMQContainer.execInContainer("rabbitmqctl", "stop_app");
    }

    /**
     * Restarts RabbitMQ container.
     */
    private void startRabbitMQ() throws Exception {
        System.out.println("Restarting RabbitMQ...");
        rabbitMQContainer.execInContainer("rabbitmqctl", "start_app");
    }

    /**
     * Waits until RabbitMQ stabilizes after restart.
     */
    private void waitUntilRabbitMQStabilizes() throws InterruptedException {
        System.out.println("Waiting for RabbitMQ to stabilize...");
        for (int i = 0; i < 10; i++) {
            if (getQueueDepth() > 0) {
                return;
            }
            Thread.sleep(2000);
        }
        throw new RuntimeException("RabbitMQ did not stabilize in time.");
    }

    /**
     * Gets the current depth of the RabbitMQ queue.
     *
     * @return the queue depth
     */
    private int getQueueDepth() {
        var properties = admin.getQueueProperties(QUEUE_NAME);
        return properties != null ? Integer.parseInt(properties.get("QUEUE_MESSAGE_COUNT").toString()) : 0;
    }

    /**
     * Logs RabbitMQ details.
     */
    private static void logRabbitMQDetails() {
        System.out.println("RabbitMQ started. UI: http://localhost:" + rabbitMQContainer.getMappedPort(15672));
        System.out.println("AMQP URL: amqp://localhost:" + rabbitMQContainer.getMappedPort(5672));
    }

    /**
     * Waits for a specified number of seconds.
     *
     * @param seconds the number of seconds to wait
     */
    private void waitFor(int seconds) throws InterruptedException {
        Thread.sleep(seconds * 1000L);
    }

    /**
     * Creates a RabbitMQ connection factory with automatic reconnection enabled.
     *
     * @return the configured connection factory
     */
    private CachingConnectionFactory createRabbitMQConnectionFactory() {
        CachingConnectionFactory factory = new CachingConnectionFactory(rabbitMQContainer.getHost(),
                rabbitMQContainer.getMappedPort(5672));
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);
        factory.getRabbitConnectionFactory().setNetworkRecoveryInterval(5000);
        factory.getRabbitConnectionFactory().setRequestedHeartbeat(30);
        return factory;
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    void tearDown() {
        container.stop();
        connectionFactory.destroy();
    }

    /**
     * Stops the RabbitMQ container after all tests.
     */
    @AfterAll
    static void stopContainer() {
        rabbitMQContainer.stop();
    }
}
