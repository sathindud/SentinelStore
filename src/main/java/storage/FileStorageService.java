package storage;

import main.ServerNode;
import replication.ReplicationManager;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileStorageService {
    private final ServerNode node;
    private final ReplicationManager replicationManager;
    private final String storagePath;

    public FileStorageService(ServerNode node, ReplicationManager replicationManager) {
        this.node = node;
        this.replicationManager = replicationManager;
        this.storagePath = node.getDataDir();
        ensureDirectoryExists();
    }
    private void ensureDirectoryExists() {
        try {
            Files.createDirectories(Paths.get(storagePath));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directory", e);
        }
    }

    public void storeFile(String filename, byte[] data) {
        // Store locally
        String filePath = Paths.get(storagePath, filename).toString();
        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(data);
            node.getFileMetadata().put(filename, node.getNodeId());

            // Replicate to backup nodes
            replicationManager.replicateFile(filename, data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to store file", e);
        }
    }

    public byte[] getFile(String filename) {
        String filePath = Paths.get(storagePath, filename).toString();
        try {
            return Files.readAllBytes(Paths.get(filePath));
        } catch (IOException e) {
            throw new RuntimeException("File not found: " + filename, e);
        }
    }
}
