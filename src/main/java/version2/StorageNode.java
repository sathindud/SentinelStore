package version2;

// StorageNode.java - Updated with automatic directory creation
import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.net.ServerSocket;
import java.net.Socket;

public class StorageNode {
    private String nodeId;
    private int port;
    private String dataDir;
    private ZkCoordinator zkCoordinator;
    private ServerSocket serverSocket;
    private ExecutorService threadPool;
    private ReplicationWorker replicationWorker;
    private HeartbeatSystem heartbeatSystem;

    private Map<String, FileMetadata> fileMetadata = new ConcurrentHashMap<>();
    private Map<String, List<FileVersion>> fileVersions = new ConcurrentHashMap<>();

    public StorageNode(String nodeId, int port, String zkAddress) throws Exception {
        this.nodeId = nodeId;
        this.port = port;
        this.dataDir = createDataDirectory(nodeId);
        this.threadPool = Executors.newFixedThreadPool(10);

        // Ensure proper address format
        String nodeAddress = "localhost:" + port;
        System.out.println("Initializing StorageNode " + nodeId + " at " + nodeAddress);

        this.zkCoordinator = new ZkCoordinator(zkAddress, nodeId, nodeAddress);
        this.replicationWorker = new ReplicationWorker(this, zkCoordinator);
        this.heartbeatSystem = new HeartbeatSystem(zkCoordinator, this);

        // Register as active node with retry
        registerWithRetry(nodeAddress);

        zkCoordinator.participateInLeaderElection();
    }

    /**
     * Register with ZooKeeper with retry logic
     */
    private void registerWithRetry(String nodeAddress) {
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                zkCoordinator.registerAsActiveNode(nodeAddress);
                System.out.println("Successfully registered node " + nodeId + " with ZooKeeper");
                return;
            } catch (Exception e) {
                System.err.println("Attempt " + (attempt + 1) + " to register node failed: " + e.getMessage());
                if (attempt == 2) {
                    throw new RuntimeException("Failed to register node after 3 attempts", e);
                }
                try {
                    Thread.sleep(2000 * (attempt + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during registration", ie);
                }
            }
        }
    }
    /**
     * Automatically creates the data directory for this node
     */
    private String createDataDirectory(String nodeId) throws IOException {
        // Create base data directory if it doesn't exist
        Path baseDir = Paths.get("data");
        if (!Files.exists(baseDir)) {
            Files.createDirectories(baseDir);
            System.out.println("Created base data directory: " + baseDir.toAbsolutePath());
        }

        // Create node-specific directory
        Path nodeDir = baseDir.resolve(nodeId);
        if (!Files.exists(nodeDir)) {
            Files.createDirectories(nodeDir);
            System.out.println("Created data directory for node " + nodeId + ": " + nodeDir.toAbsolutePath());
        } else {
            System.out.println("Using existing data directory for node " + nodeId + ": " + nodeDir.toAbsolutePath());
        }

        // Create subdirectories for better organization
        Path uploadsDir = nodeDir.resolve("uploads");
        Path tempDir = nodeDir.resolve("temp");
        Path snapshotsDir = nodeDir.resolve("snapshots");

        Files.createDirectories(uploadsDir);
        Files.createDirectories(tempDir);
        Files.createDirectories(snapshotsDir);

        System.out.println("Created subdirectories for node " + nodeId);

        return nodeDir.toString();
    }

    /**
     * Gets the upload directory path for this node
     */
    private Path getUploadsDir() {
        return Paths.get(dataDir, "uploads");
    }

    /**
     * Gets the temporary directory path for this node
     */
    private Path getTempDir() {
        return Paths.get(dataDir, "temp");
    }

    /**
     * Gets the snapshots directory path for this node
     */
    private Path getSnapshotsDir() {
        return Paths.get(dataDir, "snapshots");
    }

    /**
     * Creates a snapshot of current file metadata
     */
    public void createSnapshot() throws IOException {
        Path snapshotFile = getSnapshotsDir().resolve("snapshot-" + System.currentTimeMillis() + ".dat");

        try (ObjectOutputStream out = new ObjectOutputStream(
                new FileOutputStream(snapshotFile.toFile()))) {

            // Serialize file metadata
            out.writeObject(new HashMap<>(fileMetadata));
            out.writeObject(new HashMap<>(fileVersions));

            System.out.println("Snapshot created: " + snapshotFile.getFileName());
        }
    }

    /**
     * Loads snapshot from file (for standby nodes)
     */
    @SuppressWarnings("unchecked")
    public void loadSnapshot(Path snapshotFile) throws IOException, ClassNotFoundException {
        if (!Files.exists(snapshotFile)) {
            throw new FileNotFoundException("Snapshot file not found: " + snapshotFile);
        }

        try (ObjectInputStream in = new ObjectInputStream(
                new FileInputStream(snapshotFile.toFile()))) {

            Map<String, FileMetadata> snapshotMetadata = (Map<String, FileMetadata>) in.readObject();
            Map<String, List<FileVersion>> snapshotVersions = (Map<String, List<FileVersion>>) in.readObject();

            fileMetadata.clear();
            fileMetadata.putAll(snapshotMetadata);

            fileVersions.clear();
            fileVersions.putAll(snapshotVersions);

            System.out.println("Snapshot loaded: " + snapshotFile.getFileName() +
                    " (" + fileMetadata.size() + " files)");
        }
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        System.out.println("StorageNode " + nodeId + " started on port " + port);
        System.out.println("Data directory: " + Paths.get(dataDir).toAbsolutePath());

        // Start background services
        heartbeatSystem.start();
        replicationWorker.start();

        // Start snapshot scheduler (for active nodes)
        if (!nodeId.startsWith("standby-")) {
            startSnapshotScheduler();
        }

        System.out.println("StorageNode " + nodeId + " is ready to accept connections");

        // Monitor leadership status
        startLeadershipMonitor();

        // Accept client connections
        while (!serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(new ClientHandler(clientSocket, this));
            } catch (IOException e) {
                if (!serverSocket.isClosed()) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Monitor leadership status and handle changes
     */
    private void startLeadershipMonitor() {
        Thread monitorThread = new Thread(() -> {
            while (!serverSocket.isClosed()) {
                try {
                    boolean currentLeaderStatus = zkCoordinator.isLeader();
                    String currentLeader = zkCoordinator.getLeaderId();

                    if (currentLeaderStatus) {
                        System.out.println("✓ This node is currently the leader");
                    } else {
                        System.out.println("○ This node is a follower. Leader: " + currentLeader);
                    }

                    Thread.sleep(30000); // Check every 30 seconds
                } catch (Exception e) {
                    System.err.println("Error in leadership monitor: " + e.getMessage());
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    /**
     * Starts periodic snapshot creation
     */
    private void startSnapshotScheduler() {
        ScheduledExecutorService snapshotScheduler = Executors.newSingleThreadScheduledExecutor();

        // Create snapshot every 5 minutes
        snapshotScheduler.scheduleAtFixedRate(() -> {
            try {
                if (zkCoordinator.isLeader()) {
                    System.out.println("Leader node creating snapshot...");
                }
                createSnapshot();
            } catch (Exception e) {
                System.err.println("Error creating snapshot: " + e.getMessage());
            }
        }, 5, 5, TimeUnit.MINUTES); // Initial delay 5 min, repeat every 5 min
    }

    public String uploadFile(String filename, byte[] data, long clientTimestamp) {
        try {
            String fileId = UUID.randomUUID().toString();
            long timestamp = System.currentTimeMillis();

            FileMetadata metadata = new FileMetadata(fileId, filename,
                    data.length, timestamp, nodeId, clientTimestamp);

            // Store file in uploads directory
            Path filePath = getUploadsDir().resolve(fileId);
            Files.write(filePath, data);

            fileMetadata.put(fileId, metadata);
            fileVersions.computeIfAbsent(filename, k -> new ArrayList<>())
                    .add(new FileVersion(fileId, timestamp, nodeId));

            System.out.println("File uploaded: " + filename + " (" + data.length +
                    " bytes) as " + fileId);

            replicationWorker.queueForReplication(fileId, metadata, data);

            return fileId;
        } catch (IOException e) {
            System.err.println("Error uploading file: " + e.getMessage());
            return null;
        }
    }

    public byte[] downloadFile(String fileId) {
        try {
            Path filePath = getUploadsDir().resolve(fileId);
            if (Files.exists(filePath)) {
                byte[] data = Files.readAllBytes(filePath);
                System.out.println("File downloaded: " + fileId + " (" + data.length + " bytes)");
                return data;
            } else {
                System.out.println("File not found: " + fileId);
                return null;
            }
        } catch (IOException e) {
            System.err.println("Error downloading file: " + e.getMessage());
            return null;
        }
    }

    public FileMetadata getFileMetadata(String fileId) {
        return fileMetadata.get(fileId);
    }

    public void receiveReplication(FileMetadata metadata, byte[] data) {
        try {
            // Conflict resolution
            FileMetadata existing = fileMetadata.get(metadata.getFileId());
            if (existing != null) {
                if (shouldAcceptNewVersion(existing, metadata)) {
                    acceptNewVersion(metadata, data);
                    System.out.println("Replicated file (replaced): " + metadata.getFilename());
                } else {
                    System.out.println("Replicated file ignored (older version): " + metadata.getFilename());
                }
            } else {
                acceptNewVersion(metadata, data);
                System.out.println("Replicated file (new): " + metadata.getFilename());
            }
        } catch (IOException e) {
            System.err.println("Error receiving replication: " + e.getMessage());
        }
    }

    private boolean shouldAcceptNewVersion(FileMetadata existing, FileMetadata newVersion) {
        // Last-Writer-Wins with NTP timestamp and node ID tiebreaker
        if (newVersion.getTimestamp() > existing.getTimestamp()) {
            return true;
        } else if (newVersion.getTimestamp() == existing.getTimestamp()) {
            return newVersion.getNodeId().compareTo(existing.getNodeId()) > 0;
        }
        return false;
    }

    private void acceptNewVersion(FileMetadata metadata, byte[] data) throws IOException {
        Path filePath = getUploadsDir().resolve(metadata.getFileId());
        Files.write(filePath, data);
        fileMetadata.put(metadata.getFileId(), metadata);

        fileVersions.computeIfAbsent(metadata.getFilename(), k -> new ArrayList<>())
                .add(new FileVersion(metadata.getFileId(),
                        metadata.getTimestamp(),
                        metadata.getNodeId()));
    }

    /**
     * Gets information about all files stored on this node
     */
    public Map<String, Object> getNodeInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("nodeId", nodeId);
        info.put("dataDir", dataDir);
        info.put("fileCount", fileMetadata.size());
        info.put("totalSize", getTotalStorageSize());
        info.put("isLeader", zkCoordinator.isLeader());

        return info;
    }

    /**
     * Calculates total storage size used by this node
     */
    private long getTotalStorageSize() {
        try {
            return Files.walk(getUploadsDir())
                    .filter(p -> p.toFile().isFile())
                    .mapToLong(p -> p.toFile().length())
                    .sum();
        } catch (IOException e) {
            return 0;
        }
    }

    /**
     * Cleans up temporary files
     */
    public void cleanupTempFiles() {
        try {
            Files.walk(getTempDir())
                    .filter(p -> p.toFile().isFile())
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                        } catch (IOException e) {
                            System.err.println("Error deleting temp file: " + p);
                        }
                    });
            System.out.println("Temporary files cleaned up");
        } catch (IOException e) {
            System.err.println("Error during temp file cleanup: " + e.getMessage());
        }
    }

    public String getNodeId() { return nodeId; }
    public String getDataDir() { return dataDir; }
    public ZkCoordinator getZkCoordinator() { return zkCoordinator; }

    /**
     * Graceful shutdown with leadership cleanup
     */
    public void shutdown() {
        System.out.println("Shutting down StorageNode " + nodeId);

        try {
            // Resign leadership if this node is leader
            if (zkCoordinator.isLeader()) {
                System.out.println("Resigning leadership before shutdown...");
                zkCoordinator.resignLeadership();
            }

            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }

            if (threadPool != null) {
                threadPool.shutdown();
                if (!threadPool.awaitTermination(10, TimeUnit.SECONDS)) {
                    threadPool.shutdownNow();
                }
            }

            if (replicationWorker != null) {
                replicationWorker.stop();
            }

            if (heartbeatSystem != null) {
                heartbeatSystem.stop();
            }

            // Cleanup ZooKeeper registration
            if (zkCoordinator != null) {
                zkCoordinator.cleanup();
            }

            System.out.println("StorageNode " + nodeId + " shutdown complete");

        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: StorageNode <nodeId> <port> <zkAddress>");
            System.out.println("Example: StorageNode node-8080 8080 localhost:2181");
            return;
        }

        StorageNode node = null;
        try {
            node = new StorageNode(args[0], Integer.parseInt(args[1]), args[2]);

            // Add shutdown hook for graceful shutdown
            StorageNode finalNode = node;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (finalNode != null) {
                    finalNode.shutdown();
                }
            }));

            node.start();

        } catch (Exception e) {
            System.err.println("Failed to start StorageNode: " + e.getMessage());
            e.printStackTrace();
            if (node != null) {
                node.shutdown();
            }
        }
    }
}