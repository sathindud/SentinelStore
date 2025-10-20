import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import java.util.concurrent.Callable;
import java.util.UUID;


import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NodeTest {

    private static List<Process> nodeProcesses = new ArrayList<>();
    private static final String ZK_HOST_PORT = "localhost:2181";
    private Client client;

    private static String sharedOriginalFilename;
    private static String sharedFileContent;
    private static Path sharedOriginalFilePath;

    @BeforeAll
    static void startCluster() throws IOException, InterruptedException {
        System.out.println("--- Setting up test environment: Starting 3 nodes ---");

        // These properties are required to build the command to launch a new Java process.
        String classpath = System.getProperty("java.class.path");
        String javaHome = System.getProperty("java.home");
        String javaBin = javaHome + File.separator + "bin" + File.separator + "java";

        // Start 3 nodes, each with a unique ID and port.
        startNodeProcess(javaBin, classpath, "node1", "8001");
        startNodeProcess(javaBin, classpath, "node2", "8002");
        startNodeProcess(javaBin, classpath, "node3", "8003");
        startNodeProcess(javaBin, classpath, "node4", "8004");
        startNodeProcess(javaBin, classpath, "node5", "8005");

        System.out.println("\nWaiting 10 seconds for nodes to initialize and elect a leader...\n");
        TimeUnit.SECONDS.sleep(10);
        System.out.println("--- Test environment setup complete ---");
    }

    /**
     * Helper method to configure and start a single Node process.
     */
    private static void startNodeProcess(String javaBin, String classpath, String nodeId, String port) throws IOException {
        System.out.println("Starting Node: " + nodeId + " on port " + port);
        ProcessBuilder pb = new ProcessBuilder(
                javaBin,
                "-cp", classpath,
                "Node",
                nodeId,
                port
        );

        pb.inheritIO();
        nodeProcesses.add(pb.start());
    }

    @AfterAll
    static void stopNodes() throws InterruptedException {
        System.out.println("Shutting down cluster nodes...");
        for (Process p : nodeProcesses) {
            // "destroy()" sends the terminate signal to the process
            p.destroy();
        }

        TimeUnit.SECONDS.sleep(2);
        System.out.println("--- Teardown complete ---");
    }

    @BeforeEach
    void setUpClient() throws Exception {
        client = new Client(ZK_HOST_PORT);
    }

    @Test
    @Order(1)
    @DisplayName("Should successfully upload a file")
    void testFileUpload_Success() throws Exception {
        System.out.println("\n>>> RUNNING TEST: testFileUpload_Success <<<\n");
        // 1. Setup: Create a temporary file to upload and store its details for the download test
        sharedOriginalFilename = "test-file-" + System.currentTimeMillis() + ".txt";
        sharedFileContent = "This is a test file for the distributed system. Content: " + Math.random();
        sharedOriginalFilePath = Paths.get(sharedOriginalFilename);

        Files.write(sharedOriginalFilePath, sharedFileContent.getBytes());
        System.out.println("Created test file: " + sharedOriginalFilename);

        // 2. Find the leader
        String[] leaderInfo = client.getLeaderInfo();
        assertNotNull(leaderInfo, "Could not find leader information in ZooKeeper.");
        assertEquals(3, leaderInfo.length, "Leader info format is incorrect.");

        String leaderId = leaderInfo[0];
        String leaderHost = leaderInfo[1];
        int leaderPort = Integer.parseInt(leaderInfo[2]);
        System.out.println("Found leader '" + leaderId + "' at " + leaderHost + ":" + leaderPort);

        // 3. Action: Upload the file
        System.out.println("Attempting to upload file to leader...");
        String uploadResponse = client.uploadFile(leaderHost, leaderPort, sharedOriginalFilePath.toString());
        System.out.println("Upload response from server: " + uploadResponse);

        // 4. Assert Upload Success
        assertNotNull(uploadResponse, "Upload response was null.");
        assertTrue(uploadResponse.startsWith("SUCCESS:"), "Upload response should indicate success.");
        assertTrue(uploadResponse.contains("File ID="), "Upload response should contain a File ID.");
        assertTrue(uploadResponse.contains("Replicas=3/5"), "Upload should be replicated to a majority (3 out of 5 nodes).");
        System.out.println("\n>>> UPLOAD TEST FINISHED <<<\n");
    }

    @Test
    @Order(2)
    @DisplayName("Should successfully download the uploaded file")
    void testFileDownload_Success() throws Exception {
        System.out.println("\n>>> RUNNING TEST: testFileDownload_Success <<<\n");
        // 1. Pre-condition check: Ensure the upload test has run and provided the necessary info
        assertNotNull(sharedOriginalFilename, "Upload test must run first and set the filename.");
        assertNotNull(sharedOriginalFilePath, "Upload test must run first and set the file path.");

        // 2. Find the current leader from ZooKeeper
        System.out.println("Finding current cluster leader...");
        String[] leaderInfo = client.getLeaderInfo();
        assertNotNull(leaderInfo, "Could not find leader information in ZooKeeper.");
        assertEquals(3, leaderInfo.length, "Leader info format is incorrect.");
        String leaderId = leaderInfo[0];
        String leaderHost = leaderInfo[1];
        int leaderPort = Integer.parseInt(leaderInfo[2]);
        System.out.println("Found leader '" + leaderId + "' at " + leaderHost + ":" + leaderPort);

        try {
            // 3. To ensure we are testing the downloaded file, delete the local original copy first.
            Files.deleteIfExists(sharedOriginalFilePath);
            assertFalse(Files.exists(sharedOriginalFilePath), "Local file should be deleted before download test.");
            System.out.println("Deleted local copy of " + sharedOriginalFilename + " to ensure a clean download test.");

            // 4. Action: Download the file
            System.out.println("Attempting to download file '" + sharedOriginalFilename + "' from leader...");
            client.downloadFile(leaderHost, leaderPort, sharedOriginalFilename);

            // 5. Assert file now exists at the expected path
            assertTrue(Files.exists(sharedOriginalFilePath), "File was not downloaded successfully.");
            System.out.println("File downloaded successfully to " + sharedOriginalFilePath);

            // 6. Assert that the downloaded content matches the original content
            String downloadedContent = new String(Files.readAllBytes(sharedOriginalFilePath));
            assertEquals(sharedFileContent, downloadedContent, "Content of downloaded file does not match original file.");
            System.out.println("SUCCESS: File content verified.");

        } finally {
            // 7. Cleanup: Remove the downloaded file
            System.out.println("Cleaning up downloaded test file...");
            Files.deleteIfExists(sharedOriginalFilePath);
            System.out.println("\n>>> DOWNLOAD TEST FINISHED <<<\n");
        }
    }

    @Test
    @Order(1)
    @DisplayName("Should handle concurrent file uploads correctly")
    void testConcurrentFileUploads() throws Exception {
        System.out.println("\n>>> RUNNING TEST: testConcurrentFileUploads <<<\n");

        int numberOfConcurrentUploads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfConcurrentUploads);
        List<Future<UploadResult>> futures = new ArrayList<>();
        List<UploadTask> tasks = new ArrayList<>();

        // 1. Create and submit tasks for each concurrent upload
        for (int i = 0; i < numberOfConcurrentUploads; i++) {
            UploadTask task = new UploadTask(i);
            tasks.add(task);
            futures.add(executor.submit(task));
        }

        // 2. Wait for all uploads to complete and check their individual responses
        for (Future<UploadResult> future : futures) {
            // Get the result from each future, with a timeout to prevent tests from hanging
            UploadResult result = future.get(30, TimeUnit.SECONDS);
            assertNotNull(result, "Upload result was null.");
            assertTrue(result.response.startsWith("SUCCESS:"), "A concurrent upload failed. Response: " + result.response);
            System.out.println("Concurrent upload successful for: " + result.filename);
        }

        executor.shutdown();
        System.out.println("\nAll concurrent uploads completed. Now verifying by downloading...");

        // 3. Verify each uploaded file by downloading it and checking its content
        try {
            for (UploadTask task : tasks) {
                // Find the current leader from ZooKeeper
                String[] leaderInfo = client.getLeaderInfo();
                String leaderHost = leaderInfo[1];
                int leaderPort = Integer.parseInt(leaderInfo[2]);

                // The client downloads the file to its original name.
                Path downloadedPath = Paths.get(task.filename);
                Files.deleteIfExists(downloadedPath); // Clean up any old versions before download

                client.downloadFile(leaderHost, leaderPort, task.filename);

                // Assert that the download was successful and the content is correct
                assertTrue(Files.exists(downloadedPath), "File " + task.filename + " was not downloaded after concurrent upload.");
                String downloadedContent = new String(Files.readAllBytes(downloadedPath));
                assertEquals(task.fileContent, downloadedContent, "Content mismatch for concurrently uploaded file: " + task.filename);
                System.out.println("Verified download for: " + task.filename);
            }
        } finally {
            // 4. Cleanup all files created during this test
            System.out.println("Cleaning up files from concurrent test...");
            for (UploadTask task : tasks) {
                Files.deleteIfExists(Paths.get(task.filename));
            }
        }
        System.out.println("\n>>> CONCURRENT UPLOAD TEST FINISHED <<<\n");
    }

    /**
     * A helper class representing a single client upload task.
     * Implements Callable to be used with an ExecutorService.
     */
    private static class UploadTask implements Callable<UploadResult> {
        final String filename;
        final String fileContent;
        private final Path filePath;

        UploadTask(int id) throws IOException {
            this.filename = "concurrent-file-" + id + ".txt";
            this.fileContent = "Concurrent content for file " + id + " - " + UUID.randomUUID().toString();
            this.filePath = Paths.get(this.filename);
            Files.write(this.filePath, this.fileContent.getBytes());
        }

        @Override
        public UploadResult call() throws Exception {
            // Each thread gets its own client to simulate real concurrent users
            Client taskClient = new Client(ZK_HOST_PORT);
            String[] leaderInfo = taskClient.getLeaderInfo();
            String leaderHost = leaderInfo[1];
            int leaderPort = Integer.parseInt(leaderInfo[2]);

            String response = taskClient.uploadFile(leaderHost, leaderPort, this.filePath.toString());

            // Cleanup the local source file immediately after upload
            Files.deleteIfExists(this.filePath);

            return new UploadResult(this.filename, response);
        }
    }

    /**
     * A simple wrapper class to hold the result of an upload task.
     */
    private static class UploadResult {
        final String filename;
        final String response;

        UploadResult(String filename, String response) {
            this.filename = filename;
            this.response = response;
        }
    }



}