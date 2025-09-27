package version2;

// StorageClient.java - Fixed with better error handling
import java.io.*;
import java.net.Socket;
import java.util.*;

public class StorageClient {
    private ZkCoordinator zkCoordinator;
    private String zkAddress;
    private static final int SOCKET_TIMEOUT = 10000;

    public StorageClient(String zkAddress) throws IOException {
        this.zkAddress = zkAddress;
        this.zkCoordinator = new ZkCoordinator(zkAddress, "client-" + UUID.randomUUID());
    }



    public String uploadFile(String filename, byte[] data) throws IOException {
        System.out.println("Attempting to upload file: " + filename);

        String lastLeaderAddress = null;

        for (int attempt = 0; attempt < 5; attempt++) {
            System.out.println("Upload attempt " + (attempt + 1));

            String leaderAddress = zkCoordinator.getLeaderAddress();
            System.out.println("Leader address retrieved: '" + leaderAddress + "'");

            if (leaderAddress != null && !leaderAddress.equals(lastLeaderAddress)) {
                lastLeaderAddress = leaderAddress;

                try {
                    String[] parts = parseNodeAddress(leaderAddress);
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);

                    System.out.println("Connecting to leader at " + host + ":" + port);
                    return uploadToNode(host, port, filename, data);

                } catch (Exception e) {
                    System.err.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());

                    // If connection failed, force re-check of leader
                    lastLeaderAddress = null;

                    if (attempt == 4) {
                        throw new IOException("All upload attempts failed", e);
                    }
                }
            } else {
                System.out.println("No new leader address available, attempt " + (attempt + 1));
            }

            // Wait before retry with increasing delay
            try {
                int delay = 2000 * (attempt + 1);
                System.out.println("Waiting " + delay + "ms before retry...");
                Thread.sleep(delay);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for leader", ie);
            }
        }

        throw new IOException("No leader available after 5 attempts");
    }

    private String[] parseNodeAddress(String address) throws IOException {
        if (address == null || address.trim().isEmpty()) {
            throw new IOException("Empty node address");
        }

        System.out.println("Parsing node address: '" + address + "'");

        // Handle various address formats
        String[] parts = address.split(":");

        if (parts.length == 1) {
            // Only hostname or IP provided, no port
            String host = parts[0].trim();

            // Check if it's a node ID (like "node-8080")
            if (host.startsWith("node-") || host.startsWith("standby-")) {
                String portStr = host.replaceAll("[^0-9]", "");
                if (!portStr.isEmpty()) {
                    try {
                        int port = Integer.parseInt(portStr);
                        if (port > 0 && port <= 65535) {
                            return new String[]{"localhost", portStr};
                        }
                    } catch (NumberFormatException e) {
                        // Continue to try other formats
                    }
                }
            }

            // Try to extract port from common patterns
            if (host.matches(".*\\d{4,5}")) {
                // Contains a port number at the end
                String portStr = host.replaceAll(".*?(\\d{4,5})$", "$1");
                String hostname = host.substring(0, host.length() - portStr.length()).replaceAll("[^a-zA-Z.-]", "");
                if (hostname.isEmpty()) hostname = "localhost";

                try {
                    int port = Integer.parseInt(portStr);
                    if (port > 0 && port <= 65535) {
                        return new String[]{hostname, portStr};
                    }
                } catch (NumberFormatException e) {
                    // Continue to default
                }
            }

            // Default to localhost with common port
            return new String[]{"localhost", "8080"};

        } else if (parts.length == 2) {
            // Standard host:port format
            String host = parts[0].trim();
            String portStr = parts[1].trim();

            if (host.isEmpty()) host = "localhost";

            // Validate port
            try {
                int port = Integer.parseInt(portStr);
                if (port <= 0 || port > 65535) {
                    throw new IOException("Invalid port number: " + port);
                }
            } catch (NumberFormatException e) {
                throw new IOException("Invalid port number: " + portStr);
            }

            return new String[]{host, portStr};

        } else {
            // More than 2 parts (like IPv6 address)
            // Try to extract port from the last part
            String lastPart = parts[parts.length - 1].trim();
            try {
                int port = Integer.parseInt(lastPart);
                if (port > 0 && port <= 65535) {
                    // Reconstruct host from all parts except the last
                    String[] hostParts = Arrays.copyOf(parts, parts.length - 1);
                    String host = String.join(":", hostParts);
                    return new String[]{host, lastPart};
                }
            } catch (NumberFormatException e) {
                // Not a valid port in last part
            }

            throw new IOException("Invalid node address format. Expected 'host:port', got: " + address);
        }
    }

    public byte[] downloadFile(String fileId) throws IOException {
        System.out.println("Attempting to download file: " + fileId);

        // First try the leader
        String leaderAddress = zkCoordinator.getLeaderAddress();
        if (leaderAddress != null) {
            System.out.println("Trying leader node: " + leaderAddress);
            try {
                String[] parts = parseNodeAddress(leaderAddress);
                String host = parts[0];
                int port = Integer.parseInt(parts[1]);
                return downloadFromNode(host, port, fileId);
            } catch (IOException e) {
                System.err.println("Download from leader failed: " + e.getMessage());
            }
        }

        // If leader fails, try active nodes
        List<String> activeNodes = zkCoordinator.getActiveNodes();
        System.out.println("Available active nodes: " + activeNodes);

        if (activeNodes.isEmpty()) {
            System.out.println("No active nodes in list, trying to discover nodes...");

            // Fallback: try common ports
            return tryCommonPorts(fileId);
        }

        for (String nodeAddress : activeNodes) {
            System.out.println("Trying node: " + nodeAddress);

            for (int attempt = 0; attempt < 2; attempt++) {
                try {
                    String[] parts = parseNodeAddress(nodeAddress);
                    String host = parts[0];
                    int port = Integer.parseInt(parts[1]);

                    return downloadFromNode(host, port, fileId);

                } catch (IOException e) {
                    System.err.println("Failed to download from " + nodeAddress + " (attempt " + (attempt + 1) + "): " + e.getMessage());
                    if (attempt == 1) break; // Move to next node after 2 attempts
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during download", ie);
                }
            }
        }

        throw new IOException("Failed to download from all available nodes");
    }

    /**
     * Fallback method to try common node ports
     */
    private byte[] tryCommonPorts(String fileId) throws IOException {
        int[] commonPorts = {8080, 8081, 8082, 8083, 8084, 5140, 5141};

        for (int port : commonPorts) {
            System.out.println("Trying fallback port: " + port);
            try {
                return downloadFromNode("localhost", port, fileId);
            } catch (IOException e) {
                System.err.println("Failed on port " + port + ": " + e.getMessage());
                // Continue to next port
            }
        }

        throw new IOException("Failed to download from any common port");
    }

    private String uploadToNode(String host, int port, String filename, byte[] data) throws IOException {
        System.out.println("Uploading to " + host + ":" + port + " - File: " + filename + " Size: " + data.length + " bytes");

        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(SOCKET_TIMEOUT);

            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());

            // Send upload command
            out.writeObject("UPLOAD");
            out.flush();

            // Wait for acknowledgment (if needed) or send data immediately
            out.writeObject(filename);
            out.writeObject(data);
            out.writeLong(System.currentTimeMillis());
            out.flush();

            // Read response with timeout handling
            socket.setSoTimeout(30000); // 30 seconds for upload
            Object response = in.readObject();

            if (response instanceof String) {
                String fileId = (String) response;
                if (fileId != null && !fileId.isEmpty() && !fileId.startsWith("ERROR")) {
                    System.out.println("Upload successful, received file ID: " + fileId);
                    return fileId;
                } else {
                    throw new IOException("Upload failed: " + fileId);
                }
            } else {
                throw new IOException("Upload failed: invalid response type");
            }

        } catch (ClassNotFoundException e) {
            throw new IOException("Protocol error during upload", e);
        } catch (java.net.ConnectException e) {
            throw new IOException("Cannot connect to node " + host + ":" + port, e);
        } catch (java.net.SocketTimeoutException e) {
            throw new IOException("Upload timeout for node " + host + ":" + port, e);
        } finally {
            closeResources(socket, out, in);
        }
    }

    private byte[] downloadFromNode(String host, int port, String fileId) throws IOException {
        System.out.println("Downloading from " + host + ":" + port + " - File ID: " + fileId);

        Socket socket = null;
        ObjectOutputStream out = null;
        ObjectInputStream in = null;

        try {
            socket = new Socket(host, port);
            socket.setSoTimeout(SOCKET_TIMEOUT);

            out = new ObjectOutputStream(socket.getOutputStream());
            in = new ObjectInputStream(socket.getInputStream());

            out.writeObject("DOWNLOAD");
            out.writeObject(fileId);
            out.flush();

            Object response = in.readObject();

            if (response instanceof byte[]) {
                byte[] data = (byte[]) response;
                if (data.length > 0) {
                    System.out.println("Download successful, received " + data.length + " bytes");
                    return data;
                } else {
                    throw new IOException("File not found or empty: " + fileId);
                }
            } else if (response instanceof String) {
                String error = (String) response;
                throw new IOException("Download failed: " + error);
            } else {
                throw new IOException("Download failed: invalid response type");
            }

        } catch (ClassNotFoundException e) {
            throw new IOException("Protocol error during download", e);
        } catch (java.net.ConnectException e) {
            throw new IOException("Cannot connect to node " + host + ":" + port, e);
        } finally {
            closeResources(socket, out, in);
        }
    }

    private void closeResources(Socket socket, ObjectOutputStream out, ObjectInputStream in) {
        try {
            if (out != null) out.close();
        } catch (IOException e) {
            System.err.println("Error closing output stream: " + e.getMessage());
        }
        try {
            if (in != null) in.close();
        } catch (IOException e) {
            System.err.println("Error closing input stream: " + e.getMessage());
        }
        try {
            if (socket != null) socket.close();
        } catch (IOException e) {
            System.err.println("Error closing socket: " + e.getMessage());
        }
    }

    // Debug method to check cluster status
    public void printClusterStatus() {
        try {
            System.out.println("=== Cluster Status ===");
            System.out.println("Leader: " + zkCoordinator.getLeaderAddress());

            List<String> activeNodes = zkCoordinator.getActiveNodes();
            List<String> activeNodeIds = zkCoordinator.getActiveNodeIds();

            System.out.println("Active Nodes (" + activeNodes.size() + "): " + activeNodes);
            System.out.println("Active Node IDs: " + activeNodeIds);
            System.out.println("Standby Nodes: " + zkCoordinator.getStandbyNodes());
            System.out.println("======================");
        } catch (Exception e) {
            System.err.println("Error getting cluster status: " + e.getMessage());
        }
    }


    public static void main(String[] args) {
        StorageClient client = null;
        try {
            System.out.println("Initializing StorageClient...");
            client = new StorageClient("localhost:2181");

            // Wait for cluster to stabilize
            System.out.println("Waiting for cluster to stabilize...");
            Thread.sleep(8000);

            // Print cluster status for debugging
            client.printClusterStatus();

            // Test with a small file first
            System.out.println("Testing with small file...");
            byte[] testData = "Hello, Distributed Storage!".getBytes();
            String fileId = client.uploadFile("test_small.txt", testData);
            System.out.println("Uploaded file with ID: " + fileId);

            // Test download
            System.out.println("Testing file download...");
            byte[] downloaded = client.downloadFile(fileId);
            String content = new String(downloaded);
            System.out.println("Downloaded content: " + content);

            if (content.equals(new String(testData))) {
                System.out.println("✓ Upload and download test PASSED");

                // Test with larger file
                System.out.println("Testing with larger file...");
                byte[] largeData = new byte[1024]; // 1KB file
                Arrays.fill(largeData, (byte) 'A');
                String largeFileId = client.uploadFile("test_large.txt", largeData);
                System.out.println("Uploaded large file with ID: " + largeFileId);

            } else {
                System.out.println("✗ Upload and download test FAILED");
            }

        } catch (Exception e) {
            System.err.println("Client error: " + e.getMessage());
            e.printStackTrace();

            // Print additional debug info
            if (client != null) {
                client.printClusterStatus();
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public void close() {
        try {
            if (zkCoordinator != null) {
                zkCoordinator.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing client: " + e.getMessage());
        }
    }

    public ZkCoordinator getZkCoordinator() {
        return this.zkCoordinator;
    }
}