package version2;

// ReplicationWorker.java
import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.*;

public class ReplicationWorker {
    private StorageNode node;
    private ZkCoordinator zkCoordinator;
    private BlockingQueue<ReplicationTask> replicationQueue;
    private ExecutorService workerThreads;
    private volatile boolean running = true;

    public ReplicationWorker(StorageNode node, ZkCoordinator zkCoordinator) {
        this.node = node;
        this.zkCoordinator = zkCoordinator;
        this.replicationQueue = new LinkedBlockingQueue<>();
        this.workerThreads = Executors.newFixedThreadPool(3);
    }

    public void start() {
        for (int i = 0; i < 3; i++) {
            workerThreads.submit(this::processReplicationTasks);
        }
    }

    public void queueForReplication(String fileId, FileMetadata metadata, byte[] data) {
        replicationQueue.offer(new ReplicationTask(fileId, metadata, data));
    }

    private void processReplicationTasks() {
        while (running) {
            try {
                ReplicationTask task = replicationQueue.take();
                replicateToOtherNodes(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void replicateToOtherNodes(ReplicationTask task) {
        List<String> activeNodes = zkCoordinator.getActiveNodes();

        for (String nodeAddress : activeNodes) {
            if (nodeAddress.equals("localhost:" + getPortFromNodeId(node.getNodeId()))) {
                continue; // Skip self
            }

            try {
                replicateToNode(nodeAddress, task);
            } catch (IOException e) {
                System.err.println("Failed to replicate to " + nodeAddress + ": " + e.getMessage());
            }
        }
    }

    private void replicateToNode(String nodeAddress, ReplicationTask task) throws IOException {
        String[] parts = nodeAddress.split(":");
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);

        try (Socket socket = new Socket(host, port);
             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
             ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject("REPLICATE");
            out.writeObject(task.getMetadata());
            out.writeObject(task.getData());

            String response = (String) in.readObject();
            if (!"OK".equals(response)) {
                throw new IOException("Replication failed: " + response);
            }
        } catch (ClassNotFoundException e) {
            throw new IOException("Protocol error", e);
        }
    }

    private int getPortFromNodeId(String nodeId) {
        // Extract port from node ID (assuming format like "node-8080")
        try {
            return Integer.parseInt(nodeId.replaceAll("[^0-9]", ""));
        } catch (NumberFormatException e) {
            return 8080; // default
        }
    }

    public void stop() {
        running = false;
        workerThreads.shutdown();
    }
}

class ReplicationTask {
    private String fileId;
    private FileMetadata metadata;
    private byte[] data;

    public ReplicationTask(String fileId, FileMetadata metadata, byte[] data) {
        this.fileId = fileId;
        this.metadata = metadata;
        this.data = data;
    }

    public String getFileId() { return fileId; }
    public FileMetadata getMetadata() { return metadata; }
    public byte[] getData() { return data; }
}