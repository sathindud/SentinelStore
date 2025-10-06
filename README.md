# 🗂️ Distributed File Storage System with ZooKeeper

This project implements a **fault-tolerant distributed file storage system** using **ZooKeeper for leader election** and a **consensus algorithm** for maintaining data consistency across nodes.

Each node can act as a leader or follower.
- The **leader** handles client requests and coordinates replication.
- **Followers** synchronize with the leader and store replicas.

Files of **any type** (PDF, images, videos, documents, archives, etc.) are uploaded to a majority of nodes, ensuring fault tolerance and consistency.  
Downloads use a majority-vote consensus to determine the correct file version.

---

## 🧩 Features

- Leader election using **ZooKeeper**.
- **Universal file support** - handles any file type (PDF, JPG, PNG, MP4, ZIP, etc.)
- **Binary protocol** for reliable file transfer
- Logical clock–based ordering for concurrent requests.
- Heartbeat-based failure detection.
- File replication to **majority nodes**.
- Majority-vote consensus during download.
- Fault-tolerant recovery via logs.
- Simple client interface for upload/download.
- **Filename-based operations** - no need to remember file IDs

---

## 🧱 Prerequisites

1. **Java 17+**
2. **Apache ZooKeeper**
    - Download from: [https://zookeeper.apache.org/releases.html](https://zookeeper.apache.org/releases.html)
3. **IntelliJ IDEA (Recommended)**
    - Open the project folder and run each node from IntelliJ with runtime arguments.

---

## ⚙️ Setup Instructions

### Step 1: Start ZooKeeper
After installing ZooKeeper, first edit or create `zoo.cfg` file in the conf folder:

```bash
tickTime=2000
initLimit=10
syncLimit=5
dataDir=zookeeper/data
clientPort=2181
```

Then start the server:

```bash
zkServer.sh start
```

---

## Step 2: Run Server Nodes
You need to run 5 server nodes (each in a separate IntelliJ run instance or terminal).

1. In IntelliJ:
2. Open the `Node.java` file.
3. Click on **Run → Edit Configurations...**

Under **Program Arguments**, add:

```
<nodeName> <portNumber>
```

Example configurations:

```yaml
Node1 5001
Node2 5002
Node3 5003
Node4 5004
Node5 5005
```

Each node will:
- Connect to ZooKeeper.
- Elect a leader automatically.
- Print its role in the console.

Example output:

```
Node1 registered in ZooKeeper as localhost:5001
Node1 has been elected as Leader.
Leader Node1 synced with NTP at Tue Oct 07 14:23:45 LKT 2025
Node1 listening on port 5001
```

```
Node2 registered in ZooKeeper as localhost:5002
Node2 is a Follower.
Node2 completed log sync. Received: 0 bytes
Node2 listening on port 5002
```

---

## Step 3: Run Client

Once the nodes are running, you can run the `Client.java` to upload and download files of any type.

### Upload a File

Create a new Client instance with the following arguments:

```
upload <file_path>
```

**Examples:**

```bash
# Upload text file
upload sample.txt

# Upload PDF document
upload document.pdf

# Upload image
upload photo.jpg

# Upload video
upload video.mp4

# Upload archive
upload backup.zip
```

✅ The leader will:
- Assign a unique File ID (UUID).
- Store the file with its original filename in the log.
- Replicate it to a majority of followers.
- Display which followers received the file.

**Example output:**

```
Connected to leader: Node1 at localhost:5001

Uploading file: document.pdf
File size: 245678 bytes

Sent file: document.pdf (245678 bytes)

=== Upload Result ===
✓ File ID=a1b2c3d4-e5f6-7890-abcd-ef1234567890
✓ Filename=document.pdf
✓ Replicas=3/5
  localhost:5002->SUCCESS:Stored document.pdf
  localhost:5003->SUCCESS:Stored document.pdf
```

---

### Download a File

To download, run with the **original filename**:

```
download <filename>
```

**Examples:**

```bash
# Download using filename
download document.pdf

# Download image
download photo.jpg

# Download video
download video.mp4
```

The leader will:
- Look up the file ID from the filename in the log.
- Query all followers for the file.
- Perform majority-vote consensus on the file content (using hash comparison).
- Return the agreed-upon version.

**Example output:**

```
Connected to leader: Node1 at localhost:5001

Downloading file: document.pdf

File downloaded successfully!
Saved to: document.pdf
Size: 245678 bytes
```

**If file is not found:**

```
Download failed: File not found: nonexistent.pdf
```

**If no majority consensus:**

```
Download failed: No majority consensus (max=1/3)
```

**Note:** If a file with the same name already exists in the current directory, the client will automatically save it with a number suffix (e.g., `document_1.pdf`, `document_2.pdf`).

---

## Step 4: Node Logs

Each node keeps a local log of uploads with timestamps and filenames:

```
node_Node1_files/log.txt
```

**Example log entries:**

```
1733344942552:a1b2c3d4-e5f6-7890-abcd-ef1234567890:document.pdf
1733344955123:b2c3d4e5-f6a7-8901-bcde-f12345678901:photo.jpg
1733344967890:c3d4e5f6-a7b8-9012-cdef-123456789012:video.mp4
```

**Format:** `timestamp:fileId:filename`

---

## 📂 Project Structure

```
📦 distributed-storage
 ┣ 📜 Node.java                              # Main server node
 ┣ 📜 Client.java                            # Client application
 ┣ 📂 node_Node1_files/                      # Node 1 storage
 ┃  ┣ 📄 a1b2c3d4-e5f6-7890-abcd-...        # Stored file (UUID)
 ┃  ┣ 📄 b2c3d4e5-f6a7-8901-bcde-...        # Stored file (UUID)
 ┃  ┗ 📄 log.txt                             # Transaction log
 ┣ 📂 node_Node2_files/                      # Node 2 storage
 ┣ 📂 node_Node3_files/                      # Node 3 storage
 ┣ 📂 node_Node4_files/                      # Node 4 storage
 ┗ 📂 node_Node5_files/                      # Node 5 storage
```

---

## 🔧 Technical Details

### File Storage
- Files are stored with **UUID filenames** internally for uniqueness.
- Original filenames are preserved in the **log file** for easy retrieval.
- **Binary protocol** ensures data integrity for all file types.

### Replication Strategy
- Leader replicates to `(N/2) + 1` nodes (majority).
- For 5 nodes, files are replicated to at least 3 nodes.

### Consensus Mechanism
- Downloads use **hash-based voting** to verify file integrity.
- Majority consensus ensures the correct version is retrieved.

### Communication Protocol
- All node-to-node communication uses `DataInputStream`/`DataOutputStream`.
- Commands:
    - `UPLOAD_FILE:<filename>` - Upload file to leader
    - `DOWNLOAD_FILE:<filename>` - Download file by name
    - `STORE_FILE:<timestamp>:<fileId>:<filename>` - Replicate to follower
    - `APPEND_LOG:<timestamp>:<fileId>:<filename>` - Sync log entry
    - `QUERY_FILE:<fileId>` - Query follower for file
    - `GET_LOG` - Sync entire log from leader
    - `GET_TIME` - Clock synchronization

---

## 🧠 Design Summary

- **Consensus:** Achieved via majority voting on file replicas using hash comparison.
- **Replication:** Majority-based active replication coordinated by leader.
- **Fault Tolerance:** Uses heartbeats and ZooKeeper re-election for automatic failover.
- **Concurrency:** Handled using logical timestamps in transaction logs.
- **Clock Synchronization:** Berkeley algorithm for distributed clock sync.
- **File Types:** Supports any binary or text file format.
- **Protocol:** Binary protocol for reliable data transfer.

---

## 🎯 Usage Examples

### Uploading Various File Types

```bash
# Documents
java Client upload report.pdf
java Client upload presentation.pptx
java Client upload spreadsheet.xlsx

# Images
java Client upload diagram.png
java Client upload photo.jpg

# Archives
java Client upload backup.zip
java Client upload data.tar.gz

# Media
java Client upload recording.mp4
java Client upload audio.mp3

# Any other file
java Client upload database.db
java Client upload binary.exe
```

### Downloading Files

```bash
# Download using original filename
java Client download report.pdf
java Client download photo.jpg
java Client download backup.zip
```

---

## 🐛 Troubleshooting

### Leader Election Issues
- Ensure ZooKeeper is running: `zkServer.sh status`
- Check all nodes can connect to `localhost:2181`

### File Upload Fails
- Verify at least 3 nodes are running (for 5-node cluster)
- Check leader node console for error messages

### File Download Returns Error
- Ensure you're using the exact filename (case-sensitive)
- Verify the file was successfully uploaded
- Check that majority of nodes are online

### Connection Refused
- Verify port numbers are correct and not in use
- Ensure firewall allows connections on specified ports

---

## 👨‍💻 **Authors**

✨ Developed by the **AlertEye Distributed Systems Team** ✨

- 🧑‍💻 **T.S.D. De Zoysa** — [`it23669062@my.sliit.lk`](mailto:it23669062@my.sliit.lk)
- 👩‍💻 **D.T.D. Wijethunga** — [`it23614062@my.sliit.lk`](mailto:it23614062@my.sliit.lk)
- 👨‍💻 **Wickramaarachchi D S** — [`it23736450@my.sliit.lk`](mailto:it23736450@my.sliit.lk)
- 👨‍💻 **L.G.A.I. De Silva** — [`it23632028@my.sliit.lk`](mailto:it23632028@my.sliit.lk)

For the Distributed Systems Project.

---

## 📝 License

This project is part of an academic assignment for educational purposes.

---

## 🚀 Future Enhancements

- Web-based UI for file management
- File listing and search functionality
- File versioning and history
- Compression for large files
- Authentication and authorization
- HTTPS support for secure transfers