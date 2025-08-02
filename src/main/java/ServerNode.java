import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class ServerNode {

    private String nodeId;
    private String dataDir;
    private Map<String, String> fileMetadata;  // Stores file-to-node mappings

    public ServerNode(String nodeId, String dataDir) {
        this.nodeId = nodeId;
        this.dataDir = dataDir;
        this.fileMetadata = new HashMap<>();
        new File(dataDir).mkdirs();  // Ensure storage directory exists
    }

    // Basic file operations (to be extended by other members)
    public void storeFile(String filename, byte[] data) {
        // TODO: Implement in Replication component
    }

    public byte[] getFile(String filename) {
        // TODO: Implement in Replication component
        return null;
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public String getDataDir() { return dataDir; }
    public Map<String, String> getFileMetadata() { return fileMetadata; }

    public static void main(String[] args) {
        System.out.println("Hello World");
    }
}
