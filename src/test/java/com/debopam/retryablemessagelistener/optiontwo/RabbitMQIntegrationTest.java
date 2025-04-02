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
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Testcontainers
public class RabbitMQIntegrationTest {
    private static final String QUEUE_NAME = "testQueue";
    private static final String EXCHANGE_NAME = "testExchange";
    private static final String ROUTING_KEY = "testKey";

    static Path persistentDataPath = Path.of(System.getProperty("java.io.tmpdir"), "rabbitmq-data");
    static String rabbitMQVolume = "rabbitmq_data"; // Volume name
    RabbitAdmin admin = null;

    private static final RabbitMQContainer rabbitMQContainer =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.0.4-management")
                    .asCompatibleSubstituteFor("rabbitmq"))
                    //.withQueue(QUEUE_NAME)
                    .withFileSystemBind(persistentDataPath.toAbsolutePath().toString(), "/var/lib/rabbitmq/mnesia")
                    .withExposedPorts(5672, 15672)
                    .withPluginsEnabled("rabbitmq_management")
                    .withEnv("RABBITMQ_DEFAULT_USER", "guest")
                    .withEnv("RABBITMQ_DEFAULT_PASS", "guest")
                    .withReuse(true);

    private CachingConnectionFactory connectionFactory;
    private SimpleMessageListenerContainer container;
    private volatile AtomicReference<String> receivedMessage = new AtomicReference<>("");
    private CountDownLatch latch;
    private static final int NUM_MESSAGES = 2000;

    @BeforeAll
    static void startContainer() throws IOException {
        Properties properties = new Properties();
        properties.load(Files.newInputStream(Paths.get("src/test/resources/testcontainers.properties")));

        // Set each property as a system property
        properties.forEach((key, value) -> System.setProperty(key.toString(), value.toString()));

        rabbitMQContainer.start();
        System.out.println("RabbitMQ started.");
        System.out.println("Mapped file system : " + persistentDataPath.toAbsolutePath());
        System.out.println("RabbitMQ Host: " + rabbitMQContainer.getHost());
        System.out.println("RabbitMQ HttpUrl: " + rabbitMQContainer.getHttpUrl());
        System.out.println("RabbitMQ UI: http://localhost:" + rabbitMQContainer.getMappedPort(15672));
        System.out.println("RabbitMQ AMQP: amqp://localhost:" + rabbitMQContainer.getMappedPort(5672));
        System.out.println("RabbitMQ Port Bindings:" + rabbitMQContainer.getPortBindings());
    }

    @BeforeEach
    void setUp() {
        String host = rabbitMQContainer.getHost();
        Integer port = rabbitMQContainer.getMappedPort(5672);
        connectionFactory = createRabbitMQConnectionFactory(host, port);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // Declare queue and exchange

        admin = new RabbitAdmin(connectionFactory);
        Queue queue = new Queue(QUEUE_NAME, true); // Durable queue
        admin.declareQueue(queue);
        DirectExchange exchange = new DirectExchange(EXCHANGE_NAME);
        admin.declareExchange(exchange);
        admin.declareBinding(BindingBuilder.bind(queue).to(exchange).with(ROUTING_KEY));


        container = new ResilientRabbitMQListener(connectionFactory, Map.of("testQueue", message -> {
            try {
                Thread.sleep(Duration.of(100, ChronoUnit.MILLIS));
            } catch (InterruptedException e) {
                ;
            }
            System.out.println("Received message: " + message);
            receivedMessage.set(message);
            latch.countDown();
        }), new SimpleMeterRegistry());
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames("testQueue");

        latch = new CountDownLatch(NUM_MESSAGES);

        /*
        container.setMessageListener(message -> {
            receivedMessage = new String(message.getBody());
            latch.countDown();
        });
        */

        container.start();
    }

    @Test
    void testListenerRecoversAndProcessesMessageAfterFailure() throws Exception {
        // Step 1: Send a message before stopping RabbitMQ
        System.out.println("sending message to RabbitMQ...");
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        for (int i = 0; i < NUM_MESSAGES; i++) {
            rabbitTemplate.convertAndSend(EXCHANGE_NAME, ROUTING_KEY, "Persistent Message :" + i);
        }
        Thread.sleep(10000);
        // Step 2: Stop RabbitMQ
        System.out.println("stopping RabbitMQ..., messages :" + getQueueDepth(admin, QUEUE_NAME));
        //rabbitMQContainer.stop();
        rabbitMQContainer.execInContainer("rabbitmqctl", "stop_app");
        System.out.println("RabbitMQ stopped. Waiting before restarting...");
        Thread.sleep(10000); // Simulate downtime of 10 seconds

        // Step 3: Restart RabbitMQ
        System.out.println("RabbitMQ starting.");
        //rabbitMQContainer.start();
        rabbitMQContainer.execInContainer("rabbitmqctl", "start_app");
        Thread.sleep(10000); // Give time for RabbitMQ to stabilize
        System.out.println("After restart RabbitMQ HttpUrl: " + rabbitMQContainer.getHttpUrl());
        System.out.println("After restart RabbitMQ UI: http://localhost:" + rabbitMQContainer.getMappedPort(15672));
        admin = new RabbitAdmin(createRabbitMQConnectionFactory(rabbitMQContainer.getHost(),
                rabbitMQContainer.getMappedPort(5672)));
        System.out.println("Waiting for message processing... messages :" + getQueueDepth(admin, QUEUE_NAME));

        // Step 4: Wait for listener to receive the message
        boolean messageReceived = latch.await(5, TimeUnit.MINUTES);

        assertTrue(messageReceived, "Listener should recover and process the message");
        assertEquals("Persistent Message", receivedMessage.get(), "Listener should correctly process the persistent message");
    }

    private CachingConnectionFactory createRabbitMQConnectionFactory(String host, Integer port) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory(host, port);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");

        // Enable automatic reconnection
        connectionFactory.getRabbitConnectionFactory().setAutomaticRecoveryEnabled(true);
        connectionFactory.getRabbitConnectionFactory().setNetworkRecoveryInterval(5000);
        connectionFactory.getRabbitConnectionFactory().setRequestedHeartbeat(30);

        return connectionFactory;
    }

    @AfterEach
    void tearDown() {
        container.stop();
        connectionFactory.destroy();
    }

    @AfterAll
    static void stopContainer() {
        rabbitMQContainer.stop();
    }

    private int getQueueDepth(RabbitAdmin rabbitAdmin, String queueName) {
        java.util.Properties properties = rabbitAdmin.getQueueProperties(queueName);
        if (properties != null) {
            return Integer.parseInt(properties.get("QUEUE_MESSAGE_COUNT").toString());
        }
        return 0; // Or handle the case where the queue doesn't exist
    }
}

