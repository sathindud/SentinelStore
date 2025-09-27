import main.ServerNode;
import replication.ReplicaReceiver;
import replication.ReplicationManager;
import storage.FileStorageService;

import java.util.Arrays;

public class DistributedFileStorageTest {
    public static void main(String[] args) throws InterruptedException {
        // Create 3 nodes (1 primary, 2 replicas)
        ServerNode primary = new ServerNode("node1", "data/node1");
        ServerNode replica1 = new ServerNode("node2", "data/node2");
        ServerNode replica2 = new ServerNode("node3", "data/node3");

        // Start replica servers in separate threads
        new Thread(new ReplicaReceiver(replica1)).start();
        new Thread(new ReplicaReceiver(replica2)).start();

        // Set up replication
        ReplicationManager replicationManager = new ReplicationManager(primary, Arrays.asList(replica1, replica2));
        FileStorageService storageService = new FileStorageService(primary, replicationManager);

        // Test file storage and replication
        String testFile = "test.txt";
        byte[] testData = "Hello Distributed World!".getBytes();

        System.out.println("Storing file...");
        storageService.storeFile(testFile, testData);

        // Verify replication
        System.out.println("\nVerifying replication:");
        verifyFile(replica1, testFile, testData);
        verifyFile(replica2, testFile, testData);

        // Test failure scenario
        System.out.println("\nSigive me the code for basic file storage with replication extending this code. Also show me how to test itmulating primary failure...");
        byte[] newData = "Updated content".getBytes();
        storageService.storeFile(testFile, newData);  // Should fail for replicas

        System.out.println("Done!");
    }

    private static void verifyFile(ServerNode node, String filename, byte[] expectedData) {
        FileStorageService replicaStorage = new FileStorageService(node, null);
        byte[] actualData = replicaStorage.getFile(filename);

        if (Arrays.equals(expectedData, actualData)) {
            System.out.println("✓ " + node.getNodeId() + " has correct file");
        } else {
            System.out.println("✗ " + node.getNodeId() + " replication failed");
        }
    }
}
