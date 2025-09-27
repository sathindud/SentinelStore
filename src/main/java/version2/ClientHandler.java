package version2;

// ClientHandler.java - Fixed version
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;

public class ClientHandler implements Runnable {
    private Socket clientSocket;
    private StorageNode storageNode;

    public ClientHandler(Socket socket, StorageNode node) {
        this.clientSocket = socket;
        this.storageNode = node;
    }

    @Override
    public void run() {
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            in = new ObjectInputStream(clientSocket.getInputStream());
            out = new ObjectOutputStream(clientSocket.getOutputStream());

            String command = (String) in.readObject();
            System.out.println("Received command: " + command + " from " + clientSocket.getInetAddress());

            switch (command) {
                case "UPLOAD":
                    handleUpload(in, out);
                    break;
                case "DOWNLOAD":
                    handleDownload(in, out);
                    break;
                case "REPLICATE":
                    handleReplication(in, out);
                    break;
                case "LIST":
                    handleListFiles(in, out);
                    break;
                case "INFO":
                    handleFileInfo(in, out);
                    break;
                default:
                    out.writeObject("ERROR: Unknown command: " + command);
                    out.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                if (out != null) {
                    out.writeObject("ERROR: " + e.getMessage());
                    out.flush();
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        } finally {
            // Close resources
            try {
                if (in != null) in.close();
                if (out != null) out.close();
                if (clientSocket != null) clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleUpload(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        String filename = (String) in.readObject();
        byte[] data = (byte[]) in.readObject();
        long clientTimestamp = in.readLong();

        System.out.println("Uploading file: " + filename + " (" + data.length + " bytes)");

        String fileId = storageNode.uploadFile(filename, data, clientTimestamp);

        if (fileId != null) {
            out.writeObject(fileId);
            out.flush();
            System.out.println("Upload successful, file ID: " + fileId);
        } else {
            out.writeObject("ERROR: Upload failed");
            out.flush();
            System.out.println("Upload failed for file: " + filename);
        }
    }

    private void handleDownload(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        String fileId = (String) in.readObject();
        System.out.println("Downloading file: " + fileId);

        byte[] data = storageNode.downloadFile(fileId);

        if (data != null && data.length > 0) {
            out.writeObject(data);
            out.flush();
            System.out.println("Download successful, " + data.length + " bytes sent");
        } else {
            out.writeObject(new byte[0]); // Empty array indicates file not found
            out.flush();
            System.out.println("File not found: " + fileId);
        }
    }

    private void handleReplication(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        FileMetadata metadata = (FileMetadata) in.readObject();
        byte[] data = (byte[]) in.readObject();

        System.out.println("Replicating file: " + metadata.getFilename() + " from node: " + metadata.getNodeId());

        storageNode.receiveReplication(metadata, data);
        out.writeObject("OK");
        out.flush();
        System.out.println("Replication completed for file: " + metadata.getFileId());
    }

    private void handleListFiles(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        // Implementation for listing files (placeholder)
        out.writeObject(new ArrayList<String>());
        out.flush();
    }

    private void handleFileInfo(ObjectInputStream in, ObjectOutputStream out) throws Exception {
        String fileId = (String) in.readObject();
        FileMetadata metadata = storageNode.getFileMetadata(fileId);

        if (metadata != null) {
            out.writeObject(metadata);
        } else {
            out.writeObject("ERROR: File not found");
        }
        out.flush();
    }
}