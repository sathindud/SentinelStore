package version2;

// FileVersion.java
import java.io.Serializable;

public class FileVersion implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileId;
    private long timestamp;
    private String nodeId;

    public FileVersion(String fileId, long timestamp, String nodeId) {
        this.fileId = fileId;
        this.timestamp = timestamp;
        this.nodeId = nodeId;
    }

    // Getters
    public String getFileId() { return fileId; }
    public long getTimestamp() { return timestamp; }
    public String getNodeId() { return nodeId; }

    @Override
    public String toString() {
        return "FileVersion{fileId='" + fileId + "', timestamp=" + timestamp + ", nodeId='" + nodeId + "'}";
    }
}