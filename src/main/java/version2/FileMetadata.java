package version2;
// FileMetadata.java - Separate class file
import java.io.Serializable;

public class FileMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private String fileId;
    private String filename;
    private long size;
    private long timestamp;
    private String nodeId;
    private long clientTimestamp;

    public FileMetadata(String fileId, String filename, long size,
                        long timestamp, String nodeId, long clientTimestamp) {
        this.fileId = fileId;
        this.filename = filename;
        this.size = size;
        this.timestamp = timestamp;
        this.nodeId = nodeId;
        this.clientTimestamp = clientTimestamp;
    }

    // Getters
    public String getFileId() { return fileId; }
    public String getFilename() { return filename; }
    public long getSize() { return size; }
    public long getTimestamp() { return timestamp; }
    public String getNodeId() { return nodeId; }
    public long getClientTimestamp() { return clientTimestamp; }

    // Setters (if needed for deserialization)
    public void setFileId(String fileId) { this.fileId = fileId; }
    public void setFilename(String filename) { this.filename = filename; }
    public void setSize(long size) { this.size = size; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public void setClientTimestamp(long clientTimestamp) { this.clientTimestamp = clientTimestamp; }

    @Override
    public String toString() {
        return "FileMetadata{" +
                "fileId='" + fileId + '\'' +
                ", filename='" + filename + '\'' +
                ", size=" + size +
                ", timestamp=" + timestamp +
                ", nodeId='" + nodeId + '\'' +
                ", clientTimestamp=" + clientTimestamp +
                '}';
    }
}
