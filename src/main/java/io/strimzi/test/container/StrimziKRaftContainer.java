/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.container;

import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ContainerNetwork;
import com.groupcdg.pitest.annotations.DoNotMutate;
import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import org.apache.logging.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StrimziKRaftContainer extends GenericContainer<StrimziKRaftContainer> {

    public enum Role {
        BROKER,
        CONTROLLER,
        COMBINED;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    // class attributes
    private static final Logger LOGGER = LoggerFactory.getLogger(StrimziKRaftContainer.class);

    /**
     * The file containing the startup script.
     */
    public static final String STARTER_SCRIPT = "/testcontainers_start.sh";

    /**
     * Default Kafka port
     */
    public static final int KAFKA_PORT = 9092;
    public static final int CONTROLLER_PORT = 9094;

    /**
     * Prefix for network aliases.
     */
    protected static final String NETWORK_ALIAS_PREFIX = "broker-";
    protected static final int INTER_BROKER_LISTENER_PORT = 9091;

    /**
     * Lazy image name provider
     */
    private final CompletableFuture<String> imageNameProvider;

    // instance attributes
    private int kafkaExposedPort;
    private int controllerExposedPort;
    private boolean newController = false;
    private Map<String, String> kafkaConfigurationMap;
    private Integer nodeId;
    private Role role;
    private List<Controller> bootstrapControllers = new ArrayList<>();
    private int numLogDirs = 1;
    private String kafkaVersion;
    private Function<StrimziKRaftContainer, String> bootstrapServersProvider = c -> String.format("PLAINTEXT://%s:%s", getHost(), kafkaExposedPort);
    private Function<StrimziKRaftContainer, String> bootstrapControllersProvider = c -> String.format("PLAINTEXT://%s:%s", getHost(), kafkaExposedPort);
    private String clusterId;
    private MountableFile serverPropertiesFile;

    protected Set<String> listenerNames = new HashSet<>();

    /**
     * Image name is specified lazily automatically in {@link #doStart()} method
     */
    public StrimziKRaftContainer() {
        this(new CompletableFuture<>());
    }

    /**
     * Image name is lazily set in {@link #doStart()} method
     */
    private StrimziKRaftContainer(CompletableFuture<String> imageName) {
        super(imageName);
        this.imageNameProvider = imageName;
        // we need this shared network in case we deploy StrimziKafkaCluster which consist of `StrimziKafkaContainer`
        // instances and by default each container has its own network, which results in `Unable to resolve address: zookeeper:2181`
        super.setNetwork(Network.SHARED);
        // exposing kafka port from the container
        super.addEnv("LOG_DIR", "/tmp");
    }

    @Override
    @SuppressWarnings({"NPathComplexity", "CyclomaticComplexity"})
    @DoNotMutate
    protected void doStart() {
        if (!imageNameProvider.isDone()) {
            imageNameProvider.complete(KafkaVersionService.strimziTestContainerImageName(this.kafkaVersion));
        }

        super.setExposedPorts(List.of(KAFKA_PORT));

        super.withNetworkAliases(networkAlias());
        super.setCommand("sh", "-c", runStarterScript());
        super.doStart();
    }

    private String networkAlias() {
        return role + "-" + nodeId;
    }

    @Override
    @DoNotMutate
    public void stop() {
        super.stop();
    }

    /**
     * Allows overriding the startup script command.
     * The default is: <pre>{@code "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT}</pre>
     *
     * @return the command
     */
    protected String runStarterScript() {
        return "while [ ! -x " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT;
    }

    /**
     * Fluent method, which sets a waiting strategy to wait until the broker is ready.
     * <p>
     * This method waits for a log message in the broker log.
     * You can customize the strategy using {@link #waitingFor(WaitStrategy)}.
     *
     * @return StrimziKafkaContainer instance
     */
    @DoNotMutate
    public StrimziKRaftContainer waitForRunning() {
        if (role == Role.CONTROLLER) {
            super.waitingFor(Wait.forLogMessage(".*Recorded new KRaft controller.*", 1));
        } else {
            super.waitingFor(Wait.forLogMessage(".*Transitioning from RECOVERY to RUNNING.*", 1));
        }
        return this;
    }

    @Override
    @DoNotMutate
    protected void containerIsStarting(InspectContainerResponse containerInfo, boolean reused) {
        super.containerIsStarting(containerInfo, reused);

        kafkaExposedPort = getMappedPort(KAFKA_PORT);

        LOGGER.info("Mapped port: {}", kafkaExposedPort);

        if (bootstrapControllers.isEmpty()) {
            bootstrapControllers.add(new Controller(nodeId, networkAlias() + ":9094", ""));
        }

        String[] listenersConfig = buildListenersConfig(containerInfo);
        Properties defaultServerProperties = buildDefaultServerProperties(
            listenersConfig[0],
            listenersConfig[1]);
        String serverPropertiesWithOverride = overrideProperties(defaultServerProperties, kafkaConfigurationMap);

        // copy override file to the container
        copyFileToContainer(Transferable.of(serverPropertiesWithOverride.getBytes(StandardCharsets.UTF_8)), "/opt/kafka/config/kraft/server.properties");

        String command = "#!/bin/bash \n";
        command += " set -x \n";
        command += "cat /opt/kafka/config/kraft/server.properties \n";

        if (newController) {
            command += "bin/kafka-storage.sh format -t=\"" + clusterId + "\" --config /opt/kafka/config/kraft/server.properties --no-initial-controllers \n";
        } else {
            if (bootstrapControllers.size() == 1) {
                command += "bin/kafka-storage.sh format -t=\"" + clusterId + "\" --standalone -c /opt/kafka/config/kraft/server.properties \n";
            } else {
                command += "bin/kafka-storage.sh format -t=\"" + clusterId + "\" --initial-controllers " + initialControllers() + " -c /opt/kafka/config/kraft/server.properties \n";
            }
        }

        command += "bin/kafka-server-start.sh /opt/kafka/config/kraft/server.properties \n";


        Utils.asTransferableBytes(serverPropertiesFile).ifPresent(properties -> copyFileToContainer(
                properties,
                "/opt/kafka/config/kraft/server.properties"
        ));

        LOGGER.info("Copying command to 'STARTER_SCRIPT' script.");

        copyFileToContainer(
                Transferable.of(command.getBytes(StandardCharsets.UTF_8), 700),
                STARTER_SCRIPT
        );
    }

    private String initialControllers() {
        return bootstrapControllers.stream().map(Controller::toInitial).collect(Collectors.joining(","));
    }

    protected String extractListenerName(String bootstrapServers) {
        // extract listener name from given bootstrap servers
        String[] strings = bootstrapServers.split(":");
        if (strings.length < 3) {
            throw new IllegalArgumentException("The configured boostrap servers '" + bootstrapServers +
                    "' must be prefixed with a listener name.");
        }
        return strings[0];
    }

    /**
     * Builds the listener configurations for the Kafka broker based on the container's network settings.
     *
     * @param containerInfo Container network information.
     * @return An array containing:
     *          The 'listeners' configuration string.
     *          The 'advertised.listeners' configuration string.
     */
    protected String[] buildListenersConfig(final InspectContainerResponse containerInfo) {
        final String bootstrapServers = getBootstrapServers();
        final String bsListenerName = extractListenerName(bootstrapServers);
        final Collection<ContainerNetwork> networks = containerInfo.getNetworkSettings().getNetworks().values();
        final List<String> advertisedListenersNames = new ArrayList<>();
        final StringBuilder kafkaListeners = new StringBuilder();
        final StringBuilder advertisedListeners = new StringBuilder();

        if (role != Role.CONTROLLER) {
            // add first PLAINTEXT listener
            advertisedListeners.append(bootstrapServers);
            kafkaListeners.append(bsListenerName).append(":").append("//").append("0.0.0.0").append(":").append(KAFKA_PORT).append(",");
            listenerNames.add(bsListenerName);

            int listenerNumber = 1;
            int portNumber = INTER_BROKER_LISTENER_PORT;

            // configure advertised listeners
            for (ContainerNetwork network : networks) {
                String advertisedName = "BROKER" + listenerNumber;
                advertisedListeners.append(",")
                        .append(advertisedName)
                        .append("://")
                        .append(network.getIpAddress())
                        .append(":")
                        .append(portNumber);
                advertisedListenersNames.add(advertisedName);
                listenerNumber++;
                portNumber--;
            }

            portNumber = INTER_BROKER_LISTENER_PORT;

            // configure listeners
            for (String listener : advertisedListenersNames) {
                kafkaListeners
                        .append(listener)
                        .append("://0.0.0.0:")
                        .append(portNumber)
                        .append(",");
                listenerNames.add(listener);
                portNumber--;
            }
        }

        if (role == Role.CONTROLLER || role == Role.COMBINED) {
            String controllerListenerName = "CONTROLLER";
            kafkaListeners.append(controllerListenerName).append("://:").append(CONTROLLER_PORT);
            if (advertisedListeners.length() > 0) {
                advertisedListeners.append(",");
            }
            advertisedListeners
                    .append(controllerListenerName)
                    .append("://")
                    .append(networkAlias())
                    .append(":")
                    .append(CONTROLLER_PORT);

            String controllerClientsListenerName = "CONTROLLER_CLIENTS";
            kafkaListeners.append(",").append(controllerClientsListenerName).append("://:").append(KAFKA_PORT);
            advertisedListeners.append(",")
                    .append(controllerClientsListenerName)
                    .append("://")
                    .append(getHost())
                    .append(":")
                    .append(kafkaExposedPort);

            listenerNames.add(controllerListenerName);
            listenerNames.add(controllerClientsListenerName);
        }

        LOGGER.info("This is all advertised listeners for Kafka {}", advertisedListeners);

        return new String[] {
            kafkaListeners.toString(),
            advertisedListeners.toString()
        };
    }

    /**
     * Builds the default Kafka server properties.
     *
     * @param listeners                   the listeners configuration
     * @param advertisedListeners         the advertised listeners configuration
     * @return the default server properties
     */
    @SuppressWarnings({"JavaNCSS"})
    protected Properties buildDefaultServerProperties(final String listeners, final String advertisedListeners) {
        // Default properties for server.properties
        Properties properties = new Properties();

        // Common settings for both KRaft and non-KRaft modes
        properties.setProperty("listeners", listeners);
        if (role != Role.CONTROLLER) {
            properties.setProperty("inter.broker.listener.name", "BROKER1");
        }
        properties.setProperty("advertised.listeners", advertisedListeners);
        properties.setProperty("listener.security.protocol.map", configureListenerSecurityProtocolMap("PLAINTEXT"));
        properties.setProperty("log.dirs", logDirs());
        properties.setProperty("offsets.topic.replication.factor", "1");
        properties.setProperty("transaction.state.log.replication.factor", "1");
        properties.setProperty("transaction.state.log.min.isr", "1");

        // Add KRaft-specific settings if useKraft is enabled
        properties.setProperty("process.roles", role.toString());
        properties.setProperty("node.id", String.valueOf(nodeId));  // Use dynamic node id
        properties.setProperty("controller.quorum.bootstrap.servers", controllerQuorumBootstrapServers());
        properties.setProperty("controller.listener.names", "CONTROLLER,CONTROLLER_CLIENTS");

        return properties;
    }

    private String controllerQuorumBootstrapServers() {
        return bootstrapControllers.stream().map(c -> c.listener).collect(Collectors.joining(","));
    }

    private String logDirs() {
        List<String> logDirs = new ArrayList<>();
        for (int i = 0; i < numLogDirs; i++) {
            logDirs.add("/tmp/log-dir-" + i);
        }
        return String.join(",", logDirs);
    }

    /**
     * Configures the listener.security.protocol.map property based on the listenerNames set and the given security protocol.
     *
     * @param securityProtocol The security protocol to map each listener to (e.g., PLAINTEXT, SASL_PLAINTEXT).
     * @return The listener.security.protocol.map configuration string.
     */
    protected String configureListenerSecurityProtocolMap(String securityProtocol) {
        return this.listenerNames.stream()
            .map(listenerName -> listenerName + ":" + securityProtocol)
            .collect(Collectors.joining(","));
    }

    /**
     * Overrides the default Kafka server properties with the provided overrides.
     * If the overrides map is null or empty, it simply returns the default properties as a string.
     *
     * @param defaultProperties The default Kafka server properties.
     * @param overrides         The properties to override. Can be null.
     * @return A string representation of the combined server properties.
     */
    protected String overrideProperties(Properties defaultProperties, Map<String, String> overrides) {
        // Check if overrides are not null and not empty before applying them
        if (overrides != null && !overrides.isEmpty()) {
            overrides.forEach(defaultProperties::setProperty);
        }

        // Write properties to string
        StringWriter writer = new StringWriter();
        try {
            defaultProperties.store(writer, null);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to store Kafka server properties", e);
        }

        return writer.toString();
    }

    /**
     * Retrieves the bootstrap servers URL for Kafka clients.
     *
     * @return the bootstrap servers URL
     */
    @DoNotMutate
    public String getBootstrapServers() {
        return bootstrapServersProvider.apply(this);
    }

    public String getBootstrapControllers() {
        return bootstrapControllersProvider.apply(this);
    }

    public String getNetworkBootstrapServers() {
        return NETWORK_ALIAS_PREFIX + nodeId + ":" + INTER_BROKER_LISTENER_PORT;
    }

    /**
     * Get the cluster id. This is only supported for KRaft containers.
     * @return The cluster id.
     */
    public String getClusterId() {
        return clusterId;
    }

    /**
     * Fluent method, which sets @code{kafkaConfigurationMap}.
     *
     * @param kafkaConfigurationMap kafka configuration
     * @return StrimziKafkaContainer instance
     */
    public StrimziKRaftContainer withKafkaConfigurationMap(final Map<String, String> kafkaConfigurationMap) {
        this.kafkaConfigurationMap = kafkaConfigurationMap;
        return this;
    }

    /**
     * Fluent method that sets the node ID.
     *
     * @param nodeId the node ID
     * @return {@code StrimziKafkaContainer} instance
     */
    public StrimziKRaftContainer withNodeId(final int nodeId) {
        this.nodeId = nodeId;
        return self();
    }

    /**
     * Fluent method, which sets @code{kafkaVersion}.
     *
     * @param kafkaVersion kafka version
     * @return StrimziKafkaContainer instance
     */
    public StrimziKRaftContainer withKafkaVersion(final String kafkaVersion) {
        this.kafkaVersion = kafkaVersion;
        return self();
    }

    /**
     * Fluent method, which sets fixed exposed port.
     *
     * @param fixedPort fixed port to expose
     * @return StrimziKafkaContainer instance
     */
    public StrimziKRaftContainer withPort(final int fixedPort) {
        if (fixedPort <= 0) {
            throw new IllegalArgumentException("The fixed Kafka port must be greater than 0");
        }
        addFixedExposedPort(fixedPort, KAFKA_PORT);
        return self();
    }

    /**
     * Fluent method, copy server properties file to the container
     *
     * @param serverPropertiesFile the mountable config file
     * @return StrimziKafkaContainer instance
     */
    public StrimziKRaftContainer withServerProperties(final MountableFile serverPropertiesFile) {
        /*
         * Save a reference to the file and delay copying to the container until the container
         * is starting. This allows for `useKraft` to be set either before or after this method
         * is called.
         */
        this.serverPropertiesFile = serverPropertiesFile;
        return self();
    }

    protected StrimziKRaftContainer withClusterId(String clusterId) {
        this.clusterId = clusterId;
        return self();
    }

    public StrimziKRaftContainer withRole(Role role) {
        this.role = role;
        return self();
    }

    public StrimziKRaftContainer withBootstrapControllers(List<Controller> bootstrapControllers) {
        this.bootstrapControllers = bootstrapControllers;
        return self();
    }

    public StrimziKRaftContainer withNumLogDirs(int numLogDirs) {
        this.numLogDirs = numLogDirs;
        return self();
    }

    public StrimziKRaftContainer withNewController() {
        this.newController = true;
        return self();
    }

    /* test */ String getKafkaVersion() {
        return this.kafkaVersion;
    }

    /* test */ int getNodeId() {
        return nodeId;
    }

    public static class Controller {

        public final int nodeId;
        public final String listener;
        public final String uuid;

        public Controller(int nodeId, String listener, String uuid) {
            this.nodeId = nodeId;
            this.listener = listener;
            this.uuid = uuid;
        }

        public String toInitial() {
            return nodeId + "@" + listener + ":" + uuid;
        }
    }
}
