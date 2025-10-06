import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Node implements Watcher {

    private ZooKeeper zooKeeper;
    private String nodeId;
    private boolean isLeader = false;
    private static final String ELECTION_NAMESPACE = "/raft_election";
    private static final String LEADER_NODE = ELECTION_NAMESPACE + "/leader";
    private static final String NODES_NAMESPACE = "/raft/nodes";
    private static final String zkHostPort = "localhost:2181";

    private static final String host = "localhost";
    private int serverPort;
    private File storageDir;

    private static final List<String> clusterNodes = new ArrayList<>();

    private File logFile;

    private static final long SYNC_INTERVAL_MS = 30000;
    private long logicalClock;
    private double tickRate = 1.0;
    private double slowDownRate = 0;

    SimpleDateFormat sdf;

    public Node(String nodeId, int serverPort) throws Exception {
        this.nodeId = nodeId;
        this.serverPort = serverPort;

        storageDir = new File("node_" + nodeId + "_files");
        if (!storageDir.exists()) storageDir.mkdirs();

        logFile = new File(storageDir, "log.txt");
        if (!logFile.exists()) logFile.createNewFile();

        CountDownLatch connectedSignal = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkHostPort, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) connectedSignal.countDown();
        });
        connectedSignal.await();

        ensureBaseZnodes();
        registerNodeInZooKeeper();
        watchClusterNodes();

        startClock();
        sdf = new SimpleDateFormat("HH:mm:ss.SSS");

        attemptLeadership();

        if (!isLeader) syncLogFromLeader();

        startServer();
    }

    private void ensureBaseZnodes() throws Exception {
        ensurePath("/raft");
        ensurePath(ELECTION_NAMESPACE);
        ensurePath(NODES_NAMESPACE);
    }

    private void ensurePath(String path) throws Exception {
        Stat stat = zooKeeper.exists(path, false);
        if (stat == null) {
            zooKeeper.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    private void registerNodeInZooKeeper() throws Exception {
        String path = NODES_NAMESPACE + "/" + nodeId;
        byte[] data = (host + ":" + serverPort).getBytes();
        if (zooKeeper.exists(path, false) != null) zooKeeper.delete(path, -1);
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println(nodeId + " registered in ZooKeeper as " + new String(data));
    }

    private void watchClusterNodes() throws Exception {
        zooKeeper.getChildren(NODES_NAMESPACE, event -> {
            if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    updateClusterNodes();
                    watchClusterNodes();
                } catch (Exception e) { e.printStackTrace(); }
            }
        });
        updateClusterNodes();
    }

    private void updateClusterNodes() throws Exception {
        List<String> children = zooKeeper.getChildren(NODES_NAMESPACE, false);
        synchronized (clusterNodes) {
            clusterNodes.clear();
            for (String child : children) {
                byte[] data = zooKeeper.getData(NODES_NAMESPACE + "/" + child, false, null);
                clusterNodes.add(new String(data));
            }
        }
        System.out.println(nodeId + " sees cluster nodes: " + clusterNodes);
    }

    private void attemptLeadership() throws Exception {
        try {
            String leaderInfo = nodeId + ":" + host + ":" + serverPort;
            zooKeeper.create(LEADER_NODE, leaderInfo.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            isLeader = true;
            System.out.println(nodeId + " has been elected as Leader.");
            performLeaderDuties();
        } catch (KeeperException.NodeExistsException e) {
            isLeader = false;
            System.out.println(nodeId + " is a Follower.");
            watchLeader();
            syncClockFromLeader();
        }
    }

    private void watchLeader() throws Exception {
        zooKeeper.exists(LEADER_NODE, this);
    }

    private void performLeaderDuties() {
        syncLeaderWithNTP();

        new Thread(() -> {
            try {
                while (isLeader) {
                    System.out.println("Leader " + nodeId + " sending heartbeats...");
                    Thread.sleep(2000);
                }
            } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }).start();
    }

    private void startServer() {
        new Thread(() -> {
            try (ServerSocket serverSocket = new ServerSocket(serverPort)) {
                System.out.println(nodeId + " listening on port " + serverPort);
                while (true) {
                    Socket socket = serverSocket.accept();
                    new Thread(() -> handleConnection(socket)).start();
                }
            } catch (IOException e) { e.printStackTrace(); }
        }).start();
    }

    private void handleConnection(Socket socket) {
        try {
            DataInputStream dataIn = new DataInputStream(socket.getInputStream());
            String request = dataIn.readUTF();

            if (request == null || request.isEmpty()) {
                return;
            }

            // All responses now use DataOutputStream for consistency
            DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());

            if (isLeader && request.equals("GET_LOG")) {
                handleGetLog(socket, dataOut);

            } else if (isLeader && request.startsWith("UPLOAD_FILE:")) {
                handleUploadFile(request, dataIn, dataOut);

            } else if (isLeader && request.startsWith("DOWNLOAD_FILE:")) {
                handleDownloadFile(request, dataOut);

            } else if (isLeader && request.equals("GET_TIME")) {
                dataOut.writeUTF("TIME:" + logicalClock);
                dataOut.flush();

            } else if (!isLeader && request.startsWith("STORE_FILE:")) {
                handleStoreFile(request, dataIn, dataOut);

            } else if (!isLeader && request.startsWith("APPEND_LOG:")) {
                handleAppendLog(request, dataOut);

            } else if (!isLeader && request.startsWith("QUERY_FILE:")) {
                handleQueryFile(request, dataOut);
            }

            dataOut.close();

        } catch (IOException e) {
            System.err.println(nodeId + " error handling connection: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try {
                if (!socket.isClosed()) socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleGetLog(Socket socket, DataOutputStream dataOut) {
        try {
            byte[] logData = Files.readAllBytes(logFile.toPath());

            dataOut.writeUTF("SUCCESS");
            dataOut.writeInt(logData.length);
            dataOut.write(logData);
            dataOut.flush();

            System.out.println("Leader: Sent log file, size: " + logData.length + " bytes");

        } catch (IOException e) {
            System.err.println("Leader: Error sending log file: " + e.getMessage());
            try {
                dataOut.writeUTF("ERROR");
                dataOut.flush();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void handleUploadFile(String request, DataInputStream dataIn, DataOutputStream dataOut) throws IOException {
        try {
            // Parse: UPLOAD_FILE:<filename>
            String filename = request.substring("UPLOAD_FILE:".length()).trim();

            // Read file size and data
            int fileSize = dataIn.readInt();
            byte[] fileData = new byte[fileSize];
            dataIn.readFully(fileData);

            String fileId = UUID.randomUUID().toString();
            long requestTime = logicalClock;

            int totalNodes = clusterNodes.size();
            int majority = (totalNodes / 2) + 1;

            // Store metadata with original filename
            appendToLocalLog(fileId, filename, requestTime);
            File leaderFile = new File(storageDir, fileId);
            Files.write(leaderFile.toPath(), fileData);

            List<String> followers = pickFollowers();
            if (followers.isEmpty() && majority > 1) {
                dataOut.writeUTF("ERROR:No available followers to achieve majority");
                dataOut.flush();
                return;
            }

            for (String followerAddr : followers) {
                replicateLogToFollower(followerAddr, fileId, filename, requestTime);
            }

            int followersNeeded = majority - 1;

            if (followers.size() < followersNeeded) {
                dataOut.writeUTF("ERROR:Not enough followers. Need " + followersNeeded + " but only have " + followers.size());
                dataOut.flush();
                return;
            }

            Collections.shuffle(followers);
            List<String> selectedFollowers = followers.subList(0, followersNeeded);

            int successCount = 1;
            StringBuilder results = new StringBuilder();

            for (String followerAddr : selectedFollowers) {
                String response = forwardFileToFollower(followerAddr, fileId, filename, fileData, requestTime);
                results.append(followerAddr).append("->").append(response).append(";");
                if (response != null && response.contains("SUCCESS")) {
                    successCount++;
                }
            }

            if (successCount >= majority) {
                dataOut.writeUTF("SUCCESS:File ID=" + fileId + ";Filename=" + filename + ";Replicas=" + successCount + "/" + totalNodes + ";" + results.toString());
            } else {
                dataOut.writeUTF("ERROR:Failed majority. Only " + successCount + "/" + totalNodes + " confirmed");
            }
            dataOut.flush();

        } catch (Exception e) {
            dataOut.writeUTF("ERROR:" + e.getMessage());
            dataOut.flush();
            e.printStackTrace();
        }
    }

    private void handleDownloadFile(String request, DataOutputStream dataOut) throws IOException {
        try {
            // Parse: DOWNLOAD_FILE:<filename>
            String filename = request.substring("DOWNLOAD_FILE:".length()).trim();

            // Find fileId from filename in log
            String fileId = findFileIdByFilename(filename);

            if (fileId == null) {
                dataOut.writeUTF("ERROR:File not found: " + filename);
                dataOut.flush();
                return;
            }

            int totalNodes = clusterNodes.size();
            int expectedReplicas = (totalNodes / 2) + 1;
            int majority = (expectedReplicas / 2) + 1;

            Map<String, Integer> contentVotes = new HashMap<>();
            Map<String, byte[]> contentMap = new HashMap<>();
            int totalResponses = 0;

            File leaderFile = new File(storageDir, fileId);
            if (leaderFile.exists()) {
                byte[] leaderData = Files.readAllBytes(leaderFile.toPath());
                String hash = Arrays.hashCode(leaderData) + "";
                contentVotes.put(hash, contentVotes.getOrDefault(hash, 0) + 1);
                contentMap.put(hash, leaderData);
                totalResponses++;
            }

            List<String> followers = pickFollowers();
            for (String followerAddr : followers) {
                byte[] content = requestFileFromFollower(followerAddr, fileId);
                if (content != null) {
                    String hash = Arrays.hashCode(content) + "";
                    contentVotes.put(hash, contentVotes.getOrDefault(hash, 0) + 1);
                    contentMap.putIfAbsent(hash, content);
                    totalResponses++;
                }
            }

            String consensusHash = null;
            int maxVotes = 0;
            for (Map.Entry<String, Integer> e : contentVotes.entrySet()) {
                if (e.getValue() > maxVotes) {
                    maxVotes = e.getValue();
                    consensusHash = e.getKey();
                }
            }

            if (consensusHash != null && maxVotes >= majority) {
                byte[] fileData = contentMap.get(consensusHash);
                dataOut.writeUTF("SUCCESS");
                dataOut.writeInt(fileData.length);
                dataOut.write(fileData);
                dataOut.flush();
            } else {
                if (totalResponses == 0) {
                    dataOut.writeUTF("ERROR:No nodes have this file");
                } else {
                    dataOut.writeUTF("ERROR:No majority consensus (max=" + maxVotes + "/" + expectedReplicas + ")");
                }
                dataOut.flush();
            }

        } catch (Exception e) {
            dataOut.writeUTF("ERROR:" + e.getMessage());
            dataOut.flush();
            e.printStackTrace();
        }
    }

    private String findFileIdByFilename(String filename) {
        Set<String[]> logEntries = readLocalLogEntries();

        // Find the most recent entry with matching filename
        String foundFileId = null;
        long latestTimestamp = -1;

        for (String[] entry : logEntries) {
            if (entry.length >= 3) {
                long timestamp = Long.parseLong(entry[0]);
                String fileId = entry[1];
                String logFilename = entry[2];

                if (logFilename.equals(filename) && timestamp > latestTimestamp) {
                    latestTimestamp = timestamp;
                    foundFileId = fileId;
                }
            }
        }

        return foundFileId;
    }

    private void handleStoreFile(String request, DataInputStream dataIn, DataOutputStream dataOut) throws IOException {
        try {
            // Parse: STORE_FILE:<timestamp>:<fileId>:<filename>
            String[] parts = request.split(":", 4);
            String fileId = parts[2];
            String filename = parts[3];

            int fileSize = dataIn.readInt();
            byte[] fileData = new byte[fileSize];
            dataIn.readFully(fileData);

            Files.write(Paths.get(storageDir.getPath(), fileId), fileData);
            dataOut.writeUTF("SUCCESS:Stored " + filename + " as " + fileId);
            dataOut.flush();

        } catch (Exception e) {
            dataOut.writeUTF("ERROR:" + e.getMessage());
            dataOut.flush();
            e.printStackTrace();
        }
    }

    private void handleAppendLog(String request, DataOutputStream dataOut) throws IOException {
        try {
            // Parse: APPEND_LOG:<timestamp>:<fileId>:<filename>
            String[] parts = request.split(":", 4);
            long requestTime = Long.parseLong(parts[1]);
            String fileId = parts[2];
            String filename = parts[3];

            synchronized (logFile) {
                List<String> existing = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
                existing.add(requestTime + ":" + fileId + ":" + filename);
                existing.sort(Comparator.comparingLong(s -> Long.parseLong(s.split(":")[0])));
                Files.write(logFile.toPath(), existing, StandardCharsets.UTF_8);
            }
            dataOut.writeUTF("LOG_OK");
            dataOut.flush();

        } catch (Exception e) {
            dataOut.writeUTF("ERROR:" + e.getMessage());
            dataOut.flush();
            e.printStackTrace();
        }
    }

    private void handleQueryFile(String request, DataOutputStream dataOut) throws IOException {
        try {
            // Parse: QUERY_FILE:<fileId>
            String fileId = request.substring("QUERY_FILE:".length()).trim();
            File file = new File(storageDir, fileId);

            if (file.exists()) {
                byte[] fileData = Files.readAllBytes(file.toPath());
                dataOut.writeUTF("SUCCESS");
                dataOut.writeInt(fileData.length);
                dataOut.write(fileData);
            } else {
                dataOut.writeUTF("NOT_FOUND");
            }
            dataOut.flush();

        } catch (Exception e) {
            dataOut.writeUTF("ERROR:" + e.getMessage());
            dataOut.flush();
            e.printStackTrace();
        }
    }

    private byte[] requestFileFromFollower(String followerAddr, String fileId) {
        try {
            String[] parts = followerAddr.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(host, port);
                 DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                dataOut.writeUTF("QUERY_FILE:" + fileId);
                dataOut.flush();

                String status = dataIn.readUTF();
                if ("SUCCESS".equals(status)) {
                    int fileSize = dataIn.readInt();
                    byte[] fileData = new byte[fileSize];
                    dataIn.readFully(fileData);
                    return fileData;
                }
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    private void appendToLocalLog(String fileId, String filename, long requestTime) {
        try (FileWriter fw = new FileWriter(logFile, true)) {
            fw.write(requestTime + ":" + fileId + ":" + filename + "\n");
        } catch (IOException e) { e.printStackTrace(); }
    }

    private List<String> pickFollowers() {
        synchronized (clusterNodes) {
            List<String> followers = new ArrayList<>(clusterNodes);
            followers.remove(host + ":" + serverPort);
            return followers;
        }
    }

    private String forwardFileToFollower(String followerAddr, String fileId, String filename, byte[] fileData, long requestTime) {
        try {
            String[] parts = followerAddr.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(followerHost, followerPort);
                 DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                dataOut.writeUTF("STORE_FILE:" + requestTime + ":" + fileId + ":" + filename);
                dataOut.writeInt(fileData.length);
                dataOut.write(fileData);
                dataOut.flush();

                return dataIn.readUTF();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "Error: " + e.getMessage();
        }
    }

    private void replicateLogToFollower(String followerAddr, String fileId, String filename, long requestTime) {
        try {
            String[] parts = followerAddr.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(followerHost, followerPort);
                 DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                 DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                dataOut.writeUTF("APPEND_LOG:" + requestTime + ":" + fileId + ":" + filename);
                dataOut.flush();
                dataIn.readUTF();
            }
        } catch (Exception e) { e.printStackTrace(); }
    }

    private void syncLogFromLeader() {
        while (true) {
            try {
                Stat stat = zooKeeper.exists(LEADER_NODE, false);
                if (stat == null) {
                    System.out.println(nodeId + " waiting for leader to appear...");
                    Thread.sleep(1000);
                    continue;
                }

                if (isLeader) break;

                byte[] leaderData = zooKeeper.getData(LEADER_NODE, false, null);
                String leaderInfo = new String(leaderData);
                System.out.println(nodeId + " found leader: " + leaderInfo);

                String[] parts = leaderInfo.split(":");
                if (parts.length < 3) {
                    throw new IOException("Invalid leader data: " + leaderInfo);
                }

                String leaderHost = parts[1];
                int leaderPort = Integer.parseInt(parts[2]);

                System.out.println(nodeId + " connecting to leader at " + leaderHost + ":" + leaderPort);

                try (Socket socket = new Socket(leaderHost, leaderPort);
                     DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                     DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                    socket.setSoTimeout(10000);

                    dataOut.writeUTF("GET_LOG");
                    dataOut.flush();
                    System.out.println(nodeId + " sent GET_LOG request to leader");

                    String status = dataIn.readUTF();
                    System.out.println(nodeId + " received status: " + status);

                    if ("SUCCESS".equals(status)) {
                        int fileSize = dataIn.readInt();
                        System.out.println(nodeId + " receiving log file, size: " + fileSize + " bytes");

                        byte[] logData = new byte[fileSize];
                        dataIn.readFully(logData);

                        Files.write(logFile.toPath(), logData);
                        System.out.println(nodeId + " completed log sync. Received: " + fileSize + " bytes");
                        break;
                    } else {
                        throw new IOException("Leader responded with error status: " + status);
                    }
                }

            } catch (Exception e) {
                System.out.println(nodeId + " failed to sync log: " + e.getMessage());
                try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
            }
        }
    }

    private Set<String[]> readLocalLogEntries() {
        Set<String[]> entries = new HashSet<>();
        if (logFile.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        String[] parts = trimmed.split(":", 3);
                        if (parts.length >= 2) {
                            entries.add(parts);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return entries;
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(LEADER_NODE)) {
            System.out.println("Leader crashed. " + nodeId + " trying to become Leader...");
            try { attemptLeadership(); } catch (Exception e) { e.printStackTrace(); }
        }
    }

    private void syncLeaderWithNTP() {
        new Thread(() -> {
            while (isLeader) {
                try {
                    long ntpTime = System.currentTimeMillis();
                    synchronized (this) {
                        logicalClock = ntpTime;
                        tickRate = 1.0;
                    }
                    System.out.println("Leader " + nodeId + " synced with NTP at " + new Date(logicalClock));
                    Thread.sleep(SYNC_INTERVAL_MS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void syncClockFromLeader() {
        new Thread(() -> {
            while (!isLeader) {
                try {
                    Stat stat = zooKeeper.exists(LEADER_NODE, false);
                    if (stat == null) {
                        System.out.println(nodeId + " waiting for leader to appear for clock sync...");
                        Thread.sleep(2000);
                        continue;
                    }

                    byte[] leaderData = zooKeeper.getData(LEADER_NODE, false, null);
                    String[] parts = new String(leaderData).split(":");
                    String leaderHost = parts[1];
                    int leaderPort = Integer.parseInt(parts[2]);

                    long t0 = logicalClock;

                    try (Socket socket = new Socket(leaderHost, leaderPort);
                         DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                         DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                        dataOut.writeUTF("GET_TIME");
                        dataOut.flush();

                        String response = dataIn.readUTF();

                        long t1 = logicalClock;
                        long RTT = t1 - t0;

                        long leaderTime = Long.parseLong(response.substring(5));

                        long estimatedLeaderTime = leaderTime + RTT / 2;
                        long offset = estimatedLeaderTime - t1;

                        Date nodeTime = new Date(logicalClock);
                        Date leaderTimeDate = new Date(leaderTime);

                        if (offset > 0) {
                            logicalClock += offset;
                            tickRate = 1.0;
                            slowDownRate = 0;
                            System.out.println(nodeId + " node time " + sdf.format(nodeTime) +
                                    " leader time " + sdf.format(leaderTimeDate) +
                                    " fast-forwarded by " + offset + " ms");
                        } else if (offset < 0) {
                            slowDownRate = (double)Math.abs(offset) / SYNC_INTERVAL_MS;
                            tickRate = 1.0 - slowDownRate;
                            System.out.println(nodeId + " node time " + sdf.format(nodeTime) +
                                    " leader time " + sdf.format(leaderTimeDate) +
                                    " slowing clock with tickRate " + tickRate);
                        }
                    }

                    Thread.sleep(SYNC_INTERVAL_MS);

                } catch (Exception e) {
                    System.out.println(nodeId + " failed clock sync, retrying...");
                    try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
                }
            }
        }).start();
    }

    private void startClock() {
        logicalClock = System.currentTimeMillis();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                logicalClock += (long)(1000 * tickRate);
            }
        }).start();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Usage: Node <nodeId> <port>");
            System.exit(1);
        }

        String nodeId = args[0];
        int port = Integer.parseInt(args[1]);
        new Node(nodeId, port);

        Thread.sleep(Long.MAX_VALUE);
    }
}