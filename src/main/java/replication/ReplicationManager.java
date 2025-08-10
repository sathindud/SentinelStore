package replication;

import main.ServerNode;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.List;

public class ReplicationManager {

    private final ServerNode primaryNode;
    private final List<ServerNode> replicaNodes;


    public ReplicationManager(ServerNode primaryNode, List<ServerNode> replicaNodes) {
        this.primaryNode = primaryNode;
        this.replicaNodes = replicaNodes;
    }

    public void replicateFile(String filename, byte[] data) {
        for (ServerNode replica : replicaNodes) {
            if (!replica.getNodeId().equals(primaryNode.getNodeId())) {
                sendFileToReplica(replica, filename, data);
            }
        }
    }

    private void sendFileToReplica(ServerNode replica, String filename, byte[] data) {
        // Simple socket-based replication (in real project, use gRPC)
        try (Socket socket = new Socket("localhost", getReplicaPort(replica.getNodeId()));
             OutputStream out = socket.getOutputStream()) {

            DataOutputStream dos = new DataOutputStream(out);
            dos.writeUTF(filename);
            dos.writeInt(data.length);
            dos.write(data);
            dos.flush();

            System.out.println("Replicated " + filename + " to " + replica.getNodeId());
        } catch (IOException e) {
            System.err.println("Replication failed to " + replica.getNodeId() + ": " + e.getMessage());
        }
    }

    private int getReplicaPort(String nodeId) {
        // Simple port mapping (e.g., node1=8001, node2=8002)
        return 8000 + Integer.parseInt(nodeId.replace("node", ""));
    }
}
