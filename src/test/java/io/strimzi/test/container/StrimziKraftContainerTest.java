package io.strimzi.test.container;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.QuorumInfo;
import org.apache.kafka.clients.admin.RaftVoterEndpoint;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.strimzi.test.container.StrimziKRaftContainer.Controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StrimziKraftContainerTest {

    private static final String CLUSTER_ID = UUID.randomUUID().toString();
    private List<StrimziKRaftContainer> containers;

    @BeforeEach
    public void setUp() {
        containers = new ArrayList<>();
    }

    @AfterEach
    public void tearDown() {
        for (StrimziKRaftContainer container : containers) {
            container.stop();
        }
    }

    @Test
    public void testSingleController() throws Exception {
        StrimziKRaftContainer c1 = new StrimziKRaftContainer()
                .withClusterId(CLUSTER_ID)
                .withNodeId(1000)
                .withNumLogDirs(2)
                .withRole(StrimziKRaftContainer.Role.CONTROLLER)
                .waitForRunning();
        containers.add(c1);
        c1.start();

        String bootstrapServers = c1.getBootstrapControllers();

        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, bootstrapServers))) {
            QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(1, quorumInfo.voters().size());
            assertEquals(1000, quorumInfo.voters().get(0).replicaId());
            assertTrue(quorumInfo.observers().isEmpty());
        }
    }

    @Test
    public void testMultipleControllers() throws Exception {
        // Start 3 controllers using --initial-controllers
        int controllerCount = 3;
        List<Controller> controllers = new ArrayList<>();
        for (int i = 0; i < controllerCount; i++) {
            String nodeUuid = Uuid.randomUuid().toString();
            controllers.add(new Controller(i, "controller-" + i + ":9094", nodeUuid));
        }

        for (Controller controller : controllers) {
            StrimziKRaftContainer c = new StrimziKRaftContainer()
                    .withClusterId(CLUSTER_ID)
                    .withNodeId(controller.nodeId)
                    .withNumLogDirs(2)
                    .withRole(StrimziKRaftContainer.Role.CONTROLLER)
                    .withBootstrapControllers(controllers);
            containers.add(c);
            c.start();
        }
        List<String> bootstrapServers = new ArrayList<>();
        for (StrimziKRaftContainer container : containers) {
            container.waitForRunning();
            bootstrapServers.add(container.getBootstrapServers());
        }

        // Check the quorum has 3 voters
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, String.join(",", bootstrapServers)))) {
            QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(3, quorumInfo.voters().size());
            assertTrue(quorumInfo.observers().isEmpty());
        }

        // Start a 4th controller with --no-initial-controllers
        StrimziKRaftContainer c4 = new StrimziKRaftContainer()
                .withClusterId(CLUSTER_ID)
                .withNodeId(controllerCount)
                .withNumLogDirs(2)
                .withRole(StrimziKRaftContainer.Role.CONTROLLER)
                .withBootstrapControllers(controllers)
                .withNewController()
                .waitForRunning();
        containers.add(c4);
        bootstrapServers.add(c4.getBootstrapServers());
        c4.start();

        // Check the quorum now has 3 voters and 1 observer
        Uuid directoryId;
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, String.join(",", bootstrapServers)))) {
            QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(3, quorumInfo.voters().size());
            assertEquals(1, quorumInfo.observers().size());
            QuorumInfo.ReplicaState replica = quorumInfo.observers().get(0);
            directoryId = replica.replicaDirectoryId();
            assertNotNull(directoryId);
        }

        Map<Integer, Uuid> voters = new HashMap<>();
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, String.join(",", bootstrapServers)))) {
            // Make the observer join the quorum
            Set<RaftVoterEndpoint> voterEndpoints = Set.of(new RaftVoterEndpoint("CONTROLLER", "controller-3", 9094));
            admin.addRaftVoter(controllerCount, directoryId, voterEndpoints).all().get();

            // Check the quorum now has 4 voters
            QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(4, quorumInfo.voters().size());
            assertEquals(0, quorumInfo.observers().size());

            for (QuorumInfo.ReplicaState voter : quorumInfo.voters()) {
                voters.put(voter.replicaId(), voter.replicaDirectoryId());
            }
        }

        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG, String.join(",", bootstrapServers)))) {
            // Remove a random controller from the quorum
            int nodeIdToRemove = new Random().nextInt(voters.size());
            admin.removeRaftVoter(nodeIdToRemove, voters.get(nodeIdToRemove)).all().get();

            // Check the quorum now has 3 voters
            QuorumInfo quorumInfo = admin.describeMetadataQuorum().quorumInfo().get();
            assertEquals(3, quorumInfo.voters().size());
            for (QuorumInfo.ReplicaState replica : quorumInfo.voters()) {
                assertNotEquals(nodeIdToRemove, replica.replicaId());
            }
        }
    }

    //TODO
    // test what happens if a majority of the quorum is shutdown
    // understand intermittent failure with org.apache.kafka.common.errors.UnsupportedVersionException: Direct-to-controller communication is not supported with the current MetadataVersion.

}
