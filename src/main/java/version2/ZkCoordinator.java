package version2;

// ZkCoordinator.java
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ZkCoordinator implements Watcher {
    private ZooKeeper zk;
    private String zkAddress;
    private String basePath = "/distributed-storage";
    private String leaderPath = basePath + "/leader";
    private String nodesPath = basePath + "/nodes";
    private String standbysPath = basePath + "/standbys";

    private String currentNodeId;
    private String nodeAddress;
    private boolean isLeader = false;
    private String currentLeader;
    private Map<String, String> activeNodes = new ConcurrentHashMap<>();
    private Map<String, String> standbyNodes = new ConcurrentHashMap<>();

    private String electionNodePath;

    public ZkCoordinator(String zkAddress, String nodeId, String nodeAddress) throws IOException {
        this.zkAddress = zkAddress;
        this.currentNodeId = nodeId;
        this.nodeAddress = nodeAddress;
        connect();
        initializePaths();
    }

    // For client use - doesn't need node address
    public ZkCoordinator(String zkAddress, String nodeId) throws IOException {
        this.zkAddress = zkAddress;
        this.currentNodeId = nodeId;
        this.nodeAddress = null;
        connect();
        initializePaths();
    }

    private void connect() throws IOException {
        this.zk = new ZooKeeper(zkAddress, 3000, this);
        // Wait for connection to be established
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void initializePaths() {
        try {
            createPathIfNotExists(basePath);
            createPathIfNotExists(nodesPath);
            createPathIfNotExists(standbysPath);
            createPathIfNotExists(leaderPath);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void createPathIfNotExists(String path) throws Exception {
        if (zk.exists(path, false) == null) {
            try {
                zk.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Path already exists, ignore
            }
        }
    }

    public void participateInLeaderElection() {
        try {
            String electionPath = leaderPath + "/node-";
            this.electionNodePath = zk.create(electionPath, currentNodeId.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            System.out.println("Node " + currentNodeId + " created election node: " + electionNodePath);

            checkLeaderElection();

        } catch (Exception e) {
            System.err.println("Error participating in leader election: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void checkLeaderElection() {
        try {
            List<String> children = zk.getChildren(leaderPath, false);
            if (children.isEmpty()) {
                System.out.println("No election nodes found, becoming leader by default");
                becomeLeader();
                return;
            }

            Collections.sort(children);
            System.out.println("Election children: " + children);

            String smallestNode = children.get(0);
            String currentSequence = electionNodePath.substring(leaderPath.length() + 1);

            System.out.println("Current node sequence: " + currentSequence);
            System.out.println("Smallest node sequence: " + smallestNode);

            if (currentSequence.equals(smallestNode)) {
                becomeLeader();
            } else {
                // Find the node immediately before us
                int currentIndex = children.indexOf(currentSequence);
                if (currentIndex == -1) {
                    System.out.println("Current node not found in children list, re-electing...");
                    participateInLeaderElection();
                    return;
                }

                if (currentIndex > 0) {
                    String previousNodePath = leaderPath + "/" + children.get(currentIndex - 1);
                    watchPreviousNode(previousNodePath);
                } else {
                    // This should not happen since we already checked if we're the smallest
                    System.out.println("Unexpected: current index is 0 but we're not the smallest");
                    participateInLeaderElection();
                }
            }
        } catch (Exception e) {
            System.err.println("Error checking leader election: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void watchPreviousNode(String previousNodePath) {
        try {
            System.out.println("Node " + currentNodeId + " watching previous node: " + previousNodePath);

            Stat exists = zk.exists(previousNodePath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDeleted) {
                        System.out.println("Previous node " + previousNodePath + " deleted, re-electing...");
                        checkLeaderElection(); // Just check election, don't create new node
                    }
                }
            });

            if (exists == null) {
                // Previous node doesn't exist anymore, check election again
                System.out.println("Previous node already doesn't exist, checking election...");
                checkLeaderElection();
            }
        } catch (Exception e) {
            System.err.println("Error setting watch on previous node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void becomeLeader() {
        if (this.isLeader) {
            System.out.println("Node " + currentNodeId + " is already leader");
            return;
        }

        this.isLeader = true;
        this.currentLeader = currentNodeId;
        System.out.println("=== Node " + currentNodeId + " became leader ===");

        try {
            // Store leader info in the leader path
            String leaderInfo = currentNodeId + ":" + nodeAddress;
            System.out.println("Storing leader info: " + leaderInfo);

            // Use version -1 to overwrite any existing data
            zk.setData(leaderPath, leaderInfo.getBytes(), -1);

            // Set watch on leader path to detect leadership changes
            zk.exists(leaderPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeDataChanged) {
                        System.out.println("Leader data changed, updating leader info...");
                        updateLeaderInfo();
                    } else if (event.getType() == Event.EventType.NodeDeleted) {
                        System.out.println("Leader node deleted, re-initiating election...");
                        isLeader = false;
                        participateInLeaderElection();
                    }
                }
            });

            // Verify it was stored correctly
            byte[] storedData = zk.getData(leaderPath, false, null);
            if (storedData != null) {
                System.out.println("Verified stored leader info: '" + new String(storedData) + "'");
            }

        } catch (Exception e) {
            System.err.println("Error becoming leader: " + e.getMessage());
            e.printStackTrace();
            this.isLeader = false;
        }
    }


    private void updateLeaderInfo() {
        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(leaderPath, false, stat);
            if (data != null) {
                String leaderInfo = new String(data);
                String[] parts = leaderInfo.split(":");
                if (parts.length >= 2) {
                    currentLeader = parts[0];
                    System.out.println("Leader updated to: " + currentLeader);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void registerAsActiveNode(String nodeAddress) {
        try {
            this.nodeAddress = nodeAddress;
            String nodePath = nodesPath + "/" + currentNodeId;

            // Ensure address includes port
            if (!nodeAddress.contains(":")) {
                if (currentNodeId.startsWith("node-") || currentNodeId.startsWith("standby-")) {
                    String port = currentNodeId.replaceAll("[^0-9]", "");
                    nodeAddress = "localhost:" + port;
                } else {
                    nodeAddress = "localhost:8080"; // default
                }
                this.nodeAddress = nodeAddress;
            }

            // Store both node ID and address in proper format
            String nodeInfo = currentNodeId + ":" + nodeAddress;
            System.out.println("Registering active node: " + nodeInfo);

            // Delete existing node if it exists (cleanup)
            try {
                zk.delete(nodePath, -1);
            } catch (KeeperException.NoNodeException e) {
                // Ignore if node doesn't exist
            }

            // Create the ephemeral node
            zk.create(nodePath, nodeInfo.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            // Set watch on nodes path to track changes
            zk.getChildren(nodesPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        System.out.println("Active nodes changed, updating list...");
                        try {
                            updateNodeList();
                        } catch (Exception e) {
                            System.err.println("Error updating node list: " + e.getMessage());
                        }
                    }
                }
            });

            // Initial update of node list
            updateNodeList();

            System.out.println("Successfully registered as active node: " + currentNodeId);

        } catch (Exception e) {
            System.err.println("Error registering active node: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void registerAsStandbyNode(String nodeAddress) {
        try {
            this.nodeAddress = nodeAddress;
            String nodePath = standbysPath + "/" + currentNodeId;
            String nodeInfo = currentNodeId + ":" + nodeAddress;

            zk.create(nodePath, nodeInfo.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

            zk.getChildren(standbysPath, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getType() == Event.EventType.NodeChildrenChanged) {
                        try {
                            updateStandbyList();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            });

            updateStandbyList();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateNodeList() throws Exception {
        List<String> nodes = zk.getChildren(nodesPath, false);
        activeNodes.clear();
        System.out.println("Found " + nodes.size() + " active nodes in ZooKeeper: " + nodes);

        for (String node : nodes) {
            try {
                byte[] data = zk.getData(nodesPath + "/" + node, false, null);
                if (data != null) {
                    String nodeInfo = new String(data);
                    System.out.println("Node info for " + node + ": " + nodeInfo);

                    String[] parts = nodeInfo.split(":");
                    if (parts.length >= 2) {
                        String nodeId = parts[0];
                        String address = parts[1];

                        // Ensure address has port
                        if (!address.contains(":")) {
                            if (nodeId.startsWith("node-") || nodeId.startsWith("standby-")) {
                                String port = nodeId.replaceAll("[^0-9]", "");
                                address = "localhost:" + port;
                            }
                        }

                        activeNodes.put(nodeId, address);
                        System.out.println("Added active node: " + nodeId + " -> " + address);
                    } else {
                        System.out.println("Invalid node info format: " + nodeInfo);
                    }
                }
            } catch (KeeperException.NoNodeException e) {
                System.out.println("Node " + node + " no longer exists, skipping");
            } catch (Exception e) {
                System.err.println("Error processing node " + node + ": " + e.getMessage());
            }
        }
        System.out.println("Active nodes list updated. Total: " + activeNodes.size());
    }

    private void updateStandbyList() throws Exception {
        List<String> standbys = zk.getChildren(standbysPath, false);
        standbyNodes.clear();
        for (String standby : standbys) {
            byte[] data = zk.getData(standbysPath + "/" + standby, false, null);
            if (data != null) {
                String standbyInfo = new String(data);
                String[] parts = standbyInfo.split(":");
                if (parts.length >= 2) {
                    standbyNodes.put(parts[0], parts[1]);
                }
            }
        }
        System.out.println("Standby nodes updated: " + standbyNodes);
    }

    public String getLeaderAddress() {
        try {
            Stat stat = new Stat();
            byte[] data = zk.getData(leaderPath, false, stat);
            if (data != null && data.length > 0) {
                String leaderInfo = new String(data);
                System.out.println("Raw leader info from ZooKeeper: '" + leaderInfo + "'");

                String[] parts = leaderInfo.split(":");
                if (parts.length >= 2) {
                    String leaderAddress = parts[1]; // Get the address part
                    System.out.println("Found leader: " + parts[0] + " at " + leaderAddress);

                    // Validate address format
                    if (!leaderAddress.contains(":")) {
                        // Fix address format if needed
                        String nodeId = parts[0];
                        if (nodeId.startsWith("node-") || nodeId.startsWith("standby-")) {
                            String port = nodeId.replaceAll("[^0-9]", "");
                            leaderAddress = "localhost:" + port;
                            System.out.println("Fixed leader address to: " + leaderAddress);
                        }
                    }

                    return leaderAddress;
                } else {
                    System.out.println("Leader info format incorrect. Expected 'nodeId:address', got: " + leaderInfo);

                    // Fallback: try to get address from active nodes using the node ID
                    if (parts.length == 1) {
                        String nodeId = parts[0];
                        String address = activeNodes.get(nodeId);
                        if (address != null) {
                            System.out.println("Fallback: Found leader address from active nodes: " + address);
                            return address;
                        } else {
                            // Extract port from node ID as last resort
                            if (nodeId.startsWith("node-") || nodeId.startsWith("standby-")) {
                                String port = nodeId.replaceAll("[^0-9]", "");
                                address = "localhost:" + port;
                                System.out.println("Extracted address from node ID: " + address);
                                return address;
                            }
                        }
                    }
                }
            } else {
                System.out.println("No leader data found in ZooKeeper path: " + leaderPath);
            }
            return null;
        } catch (Exception e) {
            System.out.println("Error getting leader address: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public String getLeaderId() {
        try {
            byte[] data = zk.getData(leaderPath, false, null);
            if (data != null && data.length > 0) {
                String leaderInfo = new String(data);
                String[] parts = leaderInfo.split(":");
                return parts.length >= 1 ? parts[0] : null;
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    public List<String> getActiveNodes() {
        // Force refresh from ZooKeeper to get latest state
        try {
            updateNodeList();
        } catch (Exception e) {
            System.err.println("Error refreshing active nodes: " + e.getMessage());
        }

        List<String> nodes = new ArrayList<>(activeNodes.values());
        System.out.println("Returning " + nodes.size() + " active nodes: " + nodes);
        return nodes;
    }

    public List<String> getActiveNodeIds() {
        return new ArrayList<>(activeNodes.keySet());
    }

    public List<String> getStandbyNodes() {
        return new ArrayList<>(standbyNodes.values());
    }

    public boolean isLeader() {
        return isLeader;
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println("ZooKeeper event: " + event.getType() + " on path: " + event.getPath());
    }

    public void close() throws InterruptedException {
        if (zk != null) {
            zk.close();
        }
    }

    /**
     * Resign from leadership gracefully
     */
    public void resignLeadership() {
        if (this.isLeader) {
            System.out.println("Node " + currentNodeId + " resigning from leadership");
            this.isLeader = false;

            try {
                // Clear leader data
                zk.setData(leaderPath, new byte[0], -1);
            } catch (Exception e) {
                System.err.println("Error resigning leadership: " + e.getMessage());
            }
        }
    }

    /**
     * Clean up election node and registration
     */
    public void cleanup() {
        try {
            resignLeadership();

            // Delete election node if it exists
            if (electionNodePath != null) {
                try {
                    zk.delete(electionNodePath, -1);
                } catch (KeeperException.NoNodeException e) {
                    // Already deleted, ignore
                }
            }

            // Close ZooKeeper connection
            if (zk != null) {
                zk.close();
            }
        } catch (Exception e) {
            System.err.println("Error during cleanup: " + e.getMessage());
        }
    }

    public void debugZooKeeperState() {
        try {
            System.out.println("=== ZooKeeper Debug Information ===");

            // Check base path
            Stat baseStat = zk.exists(basePath, false);
            System.out.println("Base path exists: " + (baseStat != null));

            // Check nodes path
            Stat nodesStat = zk.exists(nodesPath, false);
            System.out.println("Nodes path exists: " + (nodesStat != null));

            if (nodesStat != null) {
                List<String> nodes = zk.getChildren(nodesPath, false);
                System.out.println("Nodes children: " + nodes);

                for (String node : nodes) {
                    String nodePath = nodesPath + "/" + node;
                    byte[] data = zk.getData(nodePath, false, null);
                    System.out.println("Node " + node + " data: " + (data != null ? new String(data) : "null"));
                }
            }

            // Check leader path
            Stat leaderStat = zk.exists(leaderPath, false);
            System.out.println("Leader path exists: " + (leaderStat != null));

            if (leaderStat != null) {
                byte[] leaderData = zk.getData(leaderPath, false, null);
                System.out.println("Leader data: " + (leaderData != null ? new String(leaderData) : "null"));
            }

            System.out.println("===================================");
        } catch (Exception e) {
            System.err.println("Error debugging ZooKeeper state: " + e.getMessage());
        }
    }

}