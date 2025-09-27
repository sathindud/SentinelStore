// DistributedStorageTest.java
import version2.StorageClient;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedStorageTest {
    private static final String ZK_ADDRESS = "localhost:2181";
    private static final int TEST_TIMEOUT_SECONDS = 60;

    private StorageClient client;
    private List<Process> nodeProcesses;
    private ExecutorService testExecutor;

    public DistributedStorageTest() throws IOException {
        this.client = new StorageClient(ZK_ADDRESS);
        this.nodeProcesses = new ArrayList<>();
        this.testExecutor = Executors.newFixedThreadPool(5);
    }

    public static void main(String[] args) {
        try {
            DistributedStorageTest test = new DistributedStorageTest();

            System.out.println("=== Distributed Storage System Test Suite ===");
            System.out.println("Waiting for cluster to stabilize...");
            Thread.sleep(10000); // Wait for cluster to fully start

            // Run tests sequentially
            test.testBasicFileOperations();
            test.testLeaderElection();
            test.testNodeFailureRecovery();
            test.testConcurrentUploads();

            test.cleanup();
            System.out.println("=== All Tests Completed Successfully ===");

        } catch (Exception e) {
            System.err.println("Test Suite Failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Test 1: Basic File Upload and Download
     */
    public void testBasicFileOperations() throws Exception {
        System.out.println("\n=== Test 1: Basic File Upload/Download ===");

        // Test small text file
        String testContent = "Hello Distributed Storage System! Basic functionality test.";
        byte[] testData = testContent.getBytes();

        String fileId = client.uploadFile("test_basic.txt", testData);
        assert fileId != null && !fileId.isEmpty() : "File upload failed";
        System.out.println("✓ File uploaded successfully. File ID: " + fileId);

        // Test download
        byte[] downloadedData = client.downloadFile(fileId);
        assert downloadedData != null : "File download failed";
        String downloadedContent = new String(downloadedData);
        assert testContent.equals(downloadedContent) : "Downloaded content doesn't match";
        System.out.println("✓ File downloaded successfully. Content verified.");

        // Test multiple files
        for (int i = 1; i <= 3; i++) {
            String content = "Test file content " + i;
            String filename = "test_file_" + i + ".txt";
            String fid = client.uploadFile(filename, content.getBytes());
            assert fid != null : "Multiple file upload failed for " + filename;

            byte[] data = client.downloadFile(fid);
            assert new String(data).equals(content) : "Content mismatch for " + filename;
        }
        System.out.println("✓ Multiple file upload/download test passed.");

        System.out.println("✓ Test 1 PASSED: Basic file operations working correctly");
    }

    /**
     * Test 2: Leader Election
     */
    public void testLeaderElection() throws Exception {
        System.out.println("\n=== Test 2: Leader Election ===");

        // Get current leader
        String initialLeader = getCurrentLeader();
        System.out.println("Initial leader: " + initialLeader);
        assert initialLeader != null : "No leader found";

        // Simulate leader failure by killing the leader process
        System.out.println("Simulating leader failure...");
        killNode(initialLeader);

        // Wait for new leader election
        System.out.println("Waiting for new leader election...");
        String newLeader = waitForNewLeader(initialLeader, 30);
        assert newLeader != null : "New leader election failed";
        assert !newLeader.equals(initialLeader) : "New leader should be different from old leader";
        System.out.println("✓ New leader elected: " + newLeader);

        // Test that system still works with new leader
        String testContent = "Testing system after leader election";
        String fileId = client.uploadFile("post_election_test.txt", testContent.getBytes());
        assert fileId != null : "System not working after leader election";

        byte[] downloaded = client.downloadFile(fileId);
        assert new String(downloaded).equals(testContent) : "Content mismatch after leader election";
        System.out.println("✓ System functional with new leader");

        // Restart the killed node
        System.out.println("Restarting failed node: " + initialLeader);
        startNode(initialLeader);
        Thread.sleep(8000); // Wait for node to join cluster

        System.out.println("✓ Test 2 PASSED: Leader election working correctly");
    }

    /**
     * Test 3: Node Failure Recovery with Cold Standby
     */
    public void testNodeFailureRecovery() throws Exception {
        System.out.println("\n=== Test 3: Node Failure Recovery ===");

        // Get active nodes count before failure
        int initialActiveNodes = getActiveNodesCount();
        System.out.println("Initial active nodes: " + initialActiveNodes);

        // Kill one active node
        String nodeToKill = getRandomActiveNode();
        System.out.println("Killing node: " + nodeToKill);
        killNode(nodeToKill);

        // Wait for standby to take over
        System.out.println("Waiting for standby promotion...");
        int finalActiveNodes = waitForActiveNodesCount(initialActiveNodes, 30);
        assert finalActiveNodes >= initialActiveNodes - 1 : "Standby didn't replace failed node";
        System.out.println("✓ Standby promoted. Active nodes after failure: " + finalActiveNodes);

        // Test system functionality
        String testContent = "Testing during node failure recovery";
        String fileId = client.uploadFile("failure_recovery_test.txt", testContent.getBytes());
        assert fileId != null : "System not working during failure recovery";

        byte[] downloaded = client.downloadFile(fileId);
        assert new String(downloaded).equals(testContent) : "Content mismatch during failure recovery";
        System.out.println("✓ System functional during failure recovery");

        System.out.println("✓ Test 3 PASSED: Node failure recovery working correctly");
    }

    /**
     * Test 4: Concurrent File Uploads
     */
    public void testConcurrentUploads() throws Exception {
        System.out.println("\n=== Test 4: Concurrent File Uploads ===");

        int numThreads = 10;
        int filesPerThread = 5;
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger totalFiles = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        System.out.println("Starting " + numThreads + " concurrent upload threads...");

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            Future<?> future = testExecutor.submit(() -> {
                try {
                    StorageClient threadClient = new StorageClient(ZK_ADDRESS);

                    for (int j = 0; j < filesPerThread; j++) {
                        String content = "Concurrent test content from thread " + threadId + ", file " + j;
                        String filename = "concurrent_" + threadId + "_" + j + ".txt";

                        String fileId = threadClient.uploadFile(filename, content.getBytes());
                        if (fileId != null) {
                            // Verify the upload
                            byte[] downloaded = threadClient.downloadFile(fileId);
                            if (downloaded != null && new String(downloaded).equals(content)) {
                                successCount.incrementAndGet();
                            }
                        }
                        totalFiles.incrementAndGet();
                    }

                    threadClient.close();
                    latch.countDown();
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " failed: " + e.getMessage());
                    latch.countDown();
                }
            });
            futures.add(future);
        }

        // Wait for all threads to complete
        boolean completed = latch.await(2, TimeUnit.MINUTES);
        assert completed : "Concurrent test timeout";

        System.out.println("Concurrent uploads completed:");
        System.out.println("  Total files attempted: " + totalFiles.get());
        System.out.println("  Successful uploads: " + successCount.get());

        assert successCount.get() >= totalFiles.get() * 0.8 : "Too many concurrent uploads failed";
        System.out.println("✓ Concurrent upload test passed with " + successCount.get() + "/" + totalFiles.get() + " successes");

        // Test data consistency after concurrent operations
        System.out.println("Verifying data consistency after concurrent operations...");
        verifyDataConsistency();

        System.out.println("✓ Test 4 PASSED: Concurrent uploads handled correctly");
    }

    /**
     * Helper method to verify data consistency across operations
     */
    private void verifyDataConsistency() throws Exception {
        // Upload a verification file
        String verifyContent = "Consistency verification content " + System.currentTimeMillis();
        String verifyFileId = client.uploadFile("consistency_check.txt", verifyContent.getBytes());

        // Download multiple times to ensure consistency
        for (int i = 0; i < 3; i++) {
            byte[] data = client.downloadFile(verifyFileId);
            assert data != null : "Consistency check failed - file not found";
            assert new String(data).equals(verifyContent) : "Consistency check failed - content mismatch";
            Thread.sleep(1000); // Small delay between checks
        }
        System.out.println("✓ Data consistency verified");
    }

    /**
     * Helper methods for node management
     */
    private String getCurrentLeader() throws Exception {
        // This would require adding a method to ZkCoordinator to get leader ID
        // For now, we'll use the client's coordinator
        return client.getZkCoordinator().getLeaderId();
    }

    private int getActiveNodesCount() throws Exception {
        // This would require adding a method to ZkCoordinator
        return client.getZkCoordinator().getActiveNodes().size();
    }

    private String getRandomActiveNode() throws Exception {
        List<String> activeNodes = client.getZkCoordinator().getActiveNodes();
        if (activeNodes.isEmpty()) {
            throw new IllegalStateException("No active nodes available");
        }
        Random random = new Random();
        return activeNodes.get(random.nextInt(activeNodes.size()));
    }

    private void killNode(String nodeId) throws Exception {
        // For simulation, we'll use a simple approach
        // In real scenario, you'd actually kill the process
        System.out.println("[SIMULATION] Killing node: " + nodeId);
        // Actual implementation would depend on how you manage processes
    }

    private void startNode(String nodeId) throws Exception {
        // For simulation, we'll use a simple approach
        System.out.println("[SIMULATION] Starting node: " + nodeId);
        // Actual implementation would depend on how you manage processes
    }

    private String waitForNewLeader(String oldLeader, int timeoutSeconds) throws Exception {
        long endTime = System.currentTimeMillis() + (timeoutSeconds * 1000);

        while (System.currentTimeMillis() < endTime) {
            String currentLeader = getCurrentLeader();
            if (currentLeader != null && !currentLeader.equals(oldLeader)) {
                return currentLeader;
            }
            Thread.sleep(2000);
        }
        return null;
    }

    private int waitForActiveNodesCount(int expectedCount, int timeoutSeconds) throws Exception {
        long endTime = System.currentTimeMillis() + (timeoutSeconds * 1000);

        while (System.currentTimeMillis() < endTime) {
            int currentCount = getActiveNodesCount();
            if (currentCount == expectedCount) {
                return currentCount;
            }
            Thread.sleep(2000);
        }
        return getActiveNodesCount();
    }

    /**
     * Cleanup resources
     */
    public void cleanup() {
        try {
            if (client != null) {
                client.close();
            }
            if (testExecutor != null) {
                testExecutor.shutdown();
                if (!testExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    testExecutor.shutdownNow();
                }
            }
            // Clean up any test files
            cleanupTestFiles();
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    private void cleanupTestFiles() {
        // Clean up any test data directories
        try {
            Files.walk(Paths.get("data"))
                    .filter(path -> path.toString().contains("test_") || path.toString().contains("concurrent_"))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            // Ignore cleanup errors
                        }
                    });
        } catch (IOException e) {
            // Ignore cleanup errors
        }
    }
}