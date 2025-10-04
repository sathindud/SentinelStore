import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
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

    private static final List<String> clusterNodes = new ArrayList<>(); // host:port

    // Leader log
    private File logFile;

    //For Clock sync

    private static final long SYNC_INTERVAL_MS = 30000;
    private long logicalClock;          // simulated time
    private double tickRate = 1.0;      // 1.0 = normal speed, >1 = fast, <1 = slow
    private double slowDownRate = 0;

    SimpleDateFormat sdf;
    public Node(String nodeId, int serverPort) throws Exception {
        this.nodeId = nodeId;
        this.serverPort = serverPort;

        // Create storage folder
        storageDir = new File("node_" + nodeId + "_files");
        if (!storageDir.exists()) storageDir.mkdirs();

        // Create log file
        logFile = new File(storageDir, "log.txt");
        if (!logFile.exists()) logFile.createNewFile();

        // Connect to ZooKeeper
        CountDownLatch connectedSignal = new CountDownLatch(1);
        this.zooKeeper = new ZooKeeper(zkHostPort, 3000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) connectedSignal.countDown();
        });
        connectedSignal.await();

        ensureBaseZnodes();
        registerNodeInZooKeeper();
        watchClusterNodes();

        //Starting the clock
        startClock();
        sdf = new SimpleDateFormat("HH:mm:ss.SSS"); // hours:minutes:seconds.milliseconds

        attemptLeadership();

        // Sync log from leader if follower
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
        //Time sync
        syncLeaderWithNTP();

        //Sending hart beats
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
        try (BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            String request = in.readLine();
            if (request == null) return;

            //Leaders code
            if (isLeader && request.startsWith("UPLOAD:")) {
                String content = request.substring(7).trim();
                String fileId = UUID.randomUUID().toString();
                long requestTime = logicalClock;

                // Log file locally
                appendToLocalLog(fileId, requestTime);

                // Forward to Majoity of followers
                List<String> followers = pickFollowers();
                if (followers.isEmpty()) {
                    out.println("No available followers to store file!");
                    return;
                }

                // replicate log entry actively
                for (String followerAddr: followers){
                    replicateLogToFollower(followerAddr, fileId, requestTime);
                }

                //Erasure coding to store the file replicate only in the majority of followers
                int majority = (clusterNodes.size() / 2) + 1;
                int followersNeeded = majority - 1; // Followers needed for majority

                if (followers.size() > followersNeeded) {
                    Collections.shuffle(followers);
                    followers = followers.subList(0, followersNeeded);
                }

                List<String> results = new ArrayList<>();
                for (String followerAddr : followers) {
                    String response = forwardToFollower(followerAddr, fileId, content, requestTime);
                    results.add(followerAddr + " -> " + response);

                }

                out.println("File ID: " + fileId + " replicated to followers:\n" + String.join("\n", results));

            }else if (isLeader && request.startsWith("DOWNLOAD")) {
                String fileId = request.split(" ")[1].trim();

                // Check if file exists in leader's log
                Set<String> logEntries = readLocalLogEntries();
                boolean found = false;
                for (String entry : logEntries) {
                    String[] parts = entry.split(":");
                    if (parts.length == 2 && parts[1].equals(fileId)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    out.println("NO_FILE_FOUND");
                    out.println("END_FILE");
                    return;
                }

                // Ask all followers for the file
                List<String> followers = pickFollowers();
                Map<String, Integer> contentVotes = new HashMap<>();

                for (String followerAddr : followers) {
                    String content = requestFileFromFollower(followerAddr, fileId);
                    if (content != null) {
                        contentVotes.put(content, contentVotes.getOrDefault(content, 0) + 1);
                    }
                }

                // Include leader itself if it has file stored
                File leaderFile = new File(storageDir, fileId + ".txt");
                if (leaderFile.exists()) {
                    String content = new String(Files.readAllBytes(leaderFile.toPath()));
                    contentVotes.put(content, contentVotes.getOrDefault(content, 0) + 1);
                }

                // Find the content with max votes (consensus)
                String consensusContent = null;
                int maxVotes = 0;
                for (Map.Entry<String, Integer> entry : contentVotes.entrySet()) {
                    if (entry.getValue() > maxVotes) {
                        maxVotes = entry.getValue();
                        consensusContent = entry.getKey();
                    }
                }

                if (consensusContent == null) {
                    out.println("NO_FILE_FOUND");
                } else {
                    out.println(consensusContent);
                }
                out.println("END_FILE");
            }else if (isLeader && request.equals("GET_LOG")) {

                try {
                    byte[] logData = Files.readAllBytes(logFile.toPath());

                    DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());

                    dataOut.writeUTF("SUCCESS");

                    dataOut.writeInt(logData.length);

                    dataOut.write(logData);
                    dataOut.flush();

                    System.out.println("Leader: Sent log file via request socket, size: " + logData.length + " bytes");

                } catch (IOException e) {
                    System.err.println("Leader: Error sending log file: " + e.getMessage());
                    try {
                        // Send error through the same channel
                        DataOutputStream dataOut = new DataOutputStream(socket.getOutputStream());
                        dataOut.writeUTF("ERROR");
                    } catch (IOException ex) {
                        ex.printStackTrace();
                    }
                }
            }else if (isLeader && request.equals("GET_TIME")) {
                out.println(logicalClock);
            }

            // Followers Code
            else if (!isLeader && request.startsWith("STORE")) {
                String[] parts = request.split(" ", 4);
                String fileId = parts[2];
                String content = parts[3];
                Files.writeString(Paths.get(storageDir.getPath(), fileId + ".txt"), content);
                out.println("Follower stored file: " + fileId);

            } else if (!isLeader && request.startsWith("APPEND_LOG:")) {
                String[] parts = request.split(":", 3);
                long requestTime = Long.parseLong(parts[1]);
                String fileId = parts[2];

                // Append in order
                synchronized (logFile) {
                    List<String> existing = Files.readAllLines(logFile.toPath(), StandardCharsets.UTF_8);
                    existing.add(requestTime + ":" + fileId);

                    // Sort by requestTime to maintain order
                    existing.sort(Comparator.comparingLong(s -> Long.parseLong(s.split(":")[0])));
                    Files.write(logFile.toPath(), existing, StandardCharsets.UTF_8);
                }
                out.println("LOG_OK");

            } else if (!isLeader && request.startsWith("QUERY_FILE")) {
                String fileId = request.split(" ")[1].trim();
                File file = new File(storageDir, fileId + ".txt");
                if (file.exists()) {
                    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                        String line;
                        while ((line = br.readLine()) != null) out.println(line);
                    }
                }
                out.println("END_FILE");
            }


        } catch (IOException e) { e.printStackTrace(); }
    }

    private String requestFileFromFollower(String followerAddr, String fileId) {
        try {
            String[] parts = followerAddr.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(host, port);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println("QUERY_FILE " + fileId);
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.equals("END_FILE")) break;
                    sb.append(line).append("\n");
                }

                String content = sb.toString().trim();
                return content.isEmpty() ? null : content;
            }
        } catch (Exception e) {
            return null;
        }
    }

    private void appendToLocalLog(String fileId, long requestTime) {
        try (FileWriter fw = new FileWriter(logFile, true)) {
            fw.write(requestTime + ":" + fileId + "\n");
        } catch (IOException e) { e.printStackTrace(); }
    }

    private List<String> pickFollowers() {
        synchronized (clusterNodes) {
            List<String> followers = new ArrayList<>(clusterNodes);
            followers.remove(host + ":" + serverPort); // exclude self
            return followers;
        }
    }

    private String forwardToFollower(String followerAddr, String fileId, String content, long requestTime) {
        try {
            String[] parts = followerAddr.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(followerHost, followerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println("STORE " + requestTime + " " + fileId + " " + content);
                return in.readLine();
            }
        } catch (Exception e) { e.printStackTrace(); return "Error forwarding to follower"; }
    }

    private void replicateLogToFollower(String followerAddr, String fileId, long requestTime) {
        try {
            String[] parts = followerAddr.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);

            try (Socket socket = new Socket(followerHost, followerPort);
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                out.println("APPEND_LOG:" + requestTime + ":" + fileId);
                in.readLine(); // wait for ACK
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
                     PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                     DataInputStream dataIn = new DataInputStream(socket.getInputStream())) {

                    // Set timeout
                    socket.setSoTimeout(10000);

                    // Send request through the socket
                    out.println("GET_LOG");
                    System.out.println(nodeId + " sent GET_LOG request to leader");

                    // Read response through the SAME socket
                    String status = dataIn.readUTF();
                    System.out.println(nodeId + " received status: " + status);

                    if ("SUCCESS".equals(status)) {
                        // Read file size and content through the SAME socket
                        int fileSize = dataIn.readInt();
                        System.out.println(nodeId + " receiving log file, size: " + fileSize + " bytes");

                        byte[] logData = new byte[fileSize];
                        dataIn.readFully(logData);

                        // Replace local log file
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


    private Set<String> readLocalLogEntries() {
        Set<String> entries = new HashSet<>();
        if (logFile.exists()) {
            try (BufferedReader br = new BufferedReader(new FileReader(logFile))) {
                String line;
                while ((line = br.readLine()) != null) {
                    entries.add(line.trim());
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

    // Leader: periodically sync with NTP
    private void syncLeaderWithNTP() {
        new Thread(() -> {
            while (isLeader) {
                try {
                    long ntpTime = System.currentTimeMillis(); // Simulated NTP
                    synchronized (this) {
                        logicalClock = ntpTime;  // reset to real NTP time
                        tickRate = 1.0;          // leader ticks normally
                    }
                    System.out.println("Leader " + nodeId + " synced with NTP at " + new Date(logicalClock));
                    Thread.sleep(SYNC_INTERVAL_MS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    // Follower: sync clock from leader using Cristian’s
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

                    long t0 = logicalClock; // send time

                    try (Socket socket = new Socket(leaderHost, leaderPort);
                         PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                         BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                        out.println("GET_TIME");
                        String response = in.readLine();

                        long t1 = logicalClock; // receive time
                        long RTT = t1 - t0;

                        long leaderTime = Long.parseLong(response);

                        long estimatedLeaderTime = leaderTime + RTT / 2;
                        long offset = estimatedLeaderTime - t1;

                        Date nodeTime = new Date(logicalClock);
                        Date leaderTimeDate = new Date(leaderTime);

                        if (offset > 0) {
                            // Behind → jump forward immediately
                            logicalClock += offset;
                            tickRate = 1.0;
                            slowDownRate = 0;
                            System.out.println(nodeId + " node time " + sdf.format(nodeTime) +
                                    " leader time " + sdf.format(leaderTimeDate) +
                                    " fast-forwarded by " + offset + " ms");
                        } else if (offset < 0) {
                            // Ahead → slow down
                            slowDownRate = (double)Math.abs(offset) / SYNC_INTERVAL_MS;
                            tickRate = 1.0 - slowDownRate; // reduce tick rate slightly
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

    // Logical clock updater
    private void startClock() {
        logicalClock = System.currentTimeMillis();

        new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000); // simulate 1s tick
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                // advance by tickRate seconds
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
