import org.apache.zookeeper.ZooKeeper;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Client {
    private static final String ELECTION_NAMESPACE = "/raft_election";
    private static final String LEADER_NODE = ELECTION_NAMESPACE + "/leader";

    private ZooKeeper zooKeeper;

    public Client(String zkHostPort) throws Exception {
        this.zooKeeper = new ZooKeeper(zkHostPort, 3000, null);
    }

    public String[] getLeaderInfo() throws Exception {
        byte[] data = zooKeeper.getData(LEADER_NODE, false, null);
        String leaderInfo = new String(data);
        // Format: "nodeId:host:port"
        return leaderInfo.split(":");
    }

    public String uploadFile(String leaderHost, int leaderPort, String filePath) throws IOException {
        Path path = Paths.get(filePath);
        String filename = path.getFileName().toString();
        byte[] fileContent = Files.readAllBytes(path);

        try (Socket socket = new Socket(leaderHost, leaderPort);
             DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
             DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            // Send upload request with filename
            dataOut.writeUTF("UPLOAD_FILE:" + filename);

            // Send file size and content
            dataOut.writeInt(fileContent.length);
            dataOut.write(fileContent);
            dataOut.flush();

            System.out.println("Sent file: " + filename + " (" + fileContent.length + " bytes)");

            // Read response
            String response = dataIn.readUTF();
            return response;
        }
    }

    public void downloadFile(String leaderHost, int leaderPort, String filename) throws IOException {
        try (Socket socket = new Socket(leaderHost, leaderPort);
             DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
             DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

            // Send download request with filename
            dataOut.writeUTF("DOWNLOAD_FILE:" + filename);
            dataOut.flush();

            // Read response
            String status = dataIn.readUTF();

            if (status.startsWith("SUCCESS")) {
                // Read file size and content
                int fileSize = dataIn.readInt();
                byte[] fileData = new byte[fileSize];
                dataIn.readFully(fileData);

                // Save to file with original filename
                Path outputFilePath = Paths.get(filename);

                // If file exists, create a new name
                if (Files.exists(outputFilePath)) {
                    String baseName = filename;
                    String extension = "";
                    int lastDot = filename.lastIndexOf('.');
                    if (lastDot > 0) {
                        baseName = filename.substring(0, lastDot);
                        extension = filename.substring(lastDot);
                    }

                    int counter = 1;
                    do {
                        outputFilePath = Paths.get(baseName + "_" + counter + extension);
                        counter++;
                    } while (Files.exists(outputFilePath));

                    System.out.println("File already exists, saving as: " + outputFilePath.getFileName());
                }

                Files.write(outputFilePath, fileData);

                System.out.println("File downloaded successfully!");
                System.out.println("Saved to: " + outputFilePath.getFileName());
                System.out.println("Size: " + fileSize + " bytes");

            } else if (status.startsWith("ERROR:")) {
                System.out.println("Download failed: " + status.substring(6));
            } else {
                System.out.println("Unexpected response: " + status);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage:");
            System.out.println("  upload <file_path>      - Upload a file to the cluster");
            System.out.println("  download <filename>     - Download a file from the cluster");
            System.out.println();
            System.out.println("Examples:");
            System.out.println("  java Client upload document.pdf");
            System.out.println("  java Client upload image.jpg");
            System.out.println("  java Client download document.pdf");
            System.out.println("  java Client download image.jpg");
            System.exit(1);
        }

        String zkHostPort = "localhost:2181";
        Client client = new Client(zkHostPort);

        try {
            String[] leaderInfo = client.getLeaderInfo();
            String leaderId = leaderInfo[0];
            String leaderHost = leaderInfo[1];
            int leaderPort = Integer.parseInt(leaderInfo[2]);

            System.out.println("Connected to leader: " + leaderId + " at " + leaderHost + ":" + leaderPort);
            System.out.println();

            String command = args[0].toLowerCase();

            if (command.equals("upload")) {
                if (args.length < 2) {
                    System.out.println("Error: Please provide a file path to upload");
                    System.exit(1);
                }

                String filePath = args[1];
                File file = new File(filePath);

                if (!file.exists()) {
                    System.out.println("Error: File not found: " + filePath);
                    System.exit(1);
                }

                if (!file.isFile()) {
                    System.out.println("Error: Path is not a file: " + filePath);
                    System.exit(1);
                }

                System.out.println("Uploading file: " + filePath);
                System.out.println("File size: " + file.length() + " bytes");
                System.out.println();

                String response = client.uploadFile(leaderHost, leaderPort, filePath);

                System.out.println();
                System.out.println("=== Upload Result ===");
                if (response.startsWith("SUCCESS:")) {
                    String[] parts = response.substring(8).split(";");
                    for (String part : parts) {
                        if (part.startsWith("File ID=")) {
                            System.out.println("✓ " + part);
                        } else if (part.startsWith("Replicas=")) {
                            System.out.println("✓ " + part);
                        } else if (!part.trim().isEmpty()) {
                            System.out.println("  " + part);
                        }
                    }
                } else if (response.startsWith("ERROR:")) {
                    System.out.println("✗ Upload failed: " + response.substring(6));
                } else {
                    System.out.println(response);
                }

            } else if (command.equals("download")) {
                if (args.length < 2) {
                    System.out.println("Error: Please provide filename to download");
                    System.out.println("Example: java Client download document.pdf");
                    System.exit(1);
                }

                String filename = args[1];

                System.out.println("Downloading file: " + filename);
                System.out.println();

                client.downloadFile(leaderHost, leaderPort, filename);

            } else {
                System.out.println("Error: Unknown command: " + command);
                System.out.println("Valid commands: upload, download");
                System.exit(1);
            }

        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}