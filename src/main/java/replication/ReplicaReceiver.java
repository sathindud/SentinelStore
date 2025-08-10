package replication;

import main.ServerNode;

import java.io.DataInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;

public class ReplicaReceiver implements Runnable{
    private final ServerNode node;
    private final int port;

    public ReplicaReceiver(ServerNode node) {
        this.node = node;
        this.port = 8000 + Integer.parseInt(node.getNodeId().replace("node", ""));
    }


    @Override
    public void run() {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("Replica " + node.getNodeId() + " listening on port " + port);

            while (true) {
                try (Socket clientSocket = serverSocket.accept();
                     InputStream in = clientSocket.getInputStream()) {

                    DataInputStream dis = new DataInputStream(in);
                    String filename = dis.readUTF();
                    int fileSize = dis.readInt();
                    byte[] fileData = new byte[fileSize];
                    dis.readFully(fileData);

                    // Store the replicated file
                    String filePath = Paths.get(node.getDataDir(), filename).toString();
                    try (FileOutputStream fos = new FileOutputStream(filePath)) {
                        fos.write(fileData);
                        node.getFileMetadata().put(filename, "replicated_from_primary");
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
