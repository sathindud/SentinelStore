import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

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

    public String uploadFile(String leaderHost, int leaderPort, String fileContent) throws IOException {
        try (Socket socket = new Socket(leaderHost, leaderPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("UPLOAD:" + fileContent);
            return in.readLine(); // fileId response
        }
    }

    public void downloadFile(String leaderHost, int leaderPort, String fileId) throws IOException{
        try (Socket socket = new Socket(leaderHost, leaderPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println("DOWNLOAD " + fileId);

            // 3. Read multi-line response until END_FILE
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equals("END_FILE")) break;
                sb.append(line).append("\n");
            }

            String fileContent = sb.toString().trim();
            if (fileContent.isEmpty()) {
                System.out.println("File not found on any node!");
            } else {
                System.out.println("File content received:\n" + fileContent);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        String zkHostPort = "localhost:2181";

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter file content: ");
        String fileContent = scanner.nextLine();

        Client client = new Client(zkHostPort);
        String[] leaderInfo = client.getLeaderInfo();

        String leaderId = leaderInfo[0];
        String leaderHost = leaderInfo[1];
        int leaderPort = Integer.parseInt(leaderInfo[2]);

        System.out.println("Leader elected: " + leaderId + " at " + leaderHost + ":" + leaderPort);

//        String response = client.uploadFile(leaderHost, leaderPort, fileContent);
//        System.out.println("Client got response: " + response);

        client.downloadFile(leaderHost,leaderPort, "c2290a0e-17cc-4ca6-b58d-a0239b5285ea");

    }
}
