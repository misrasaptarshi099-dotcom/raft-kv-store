package com.raft.client;

import com.raft.rpc.RpcClient;
import com.raft.rpc.RpcMessage;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class KVMemberClient {
    private int currentPort;
    private final List<Integer> clusterPorts;

    public KVMemberClient(int initialPort, List<Integer> clusterPorts) {
        this.currentPort = initialPort;
        this.clusterPorts = clusterPorts;
    }

    public void run() {
        System.out.println("=================================================");
        System.out.println("   Raft Replicated KV Store CLI Client (REPL)    ");
        System.out.println("   Connecting to entry port: " + currentPort);
        System.out.println("   Commands: PUT <key> <val>, GET <key>, DELETE <key>");
        System.out.println("             STATUS, CLUSTER, HELP, EXIT");
        System.out.println("=================================================");

        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));

        while (true) {
            try {
                System.out.print("raft-kv:" + currentPort + "> ");
                String line = console.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split("\\s+");
                String cmd = parts[0].toUpperCase();

                if ("EXIT".equals(cmd) || "QUIT".equals(cmd)) {
                    System.out.println("Goodbye.");
                    break;
                } else if ("HELP".equals(cmd)) {
                    printHelp();
                } else if ("STATUS".equals(cmd)) {
                    showNodeStatus(currentPort);
                } else if ("CLUSTER".equals(cmd)) {
                    showClusterStatus();
                } else if ("PUT".equals(cmd) || "GET".equals(cmd) || "DELETE".equals(cmd)) {
                    executeCommandWithRedirects(line);
                } else {
                    System.out.println("Unknown command: " + cmd + ". Type HELP for instructions.");
                }
            } catch (Exception e) {
                System.err.println("Client Error: " + e.getMessage());
            }
        }
    }

    private void printHelp() {
        System.out.println("Usage:");
        System.out.println("  PUT <key> <value>  - Insert/update a key with a value (requires leader consensus)");
        System.out.println("  GET <key>          - Read a key's value (routed through leader)");
        System.out.println("  DELETE <key>       - Delete a key (requires leader consensus)");
        System.out.println("  STATUS             - Query internals of the currently targeted node");
        System.out.println("  CLUSTER            - Query internals of all cluster nodes (8001-8005)");
        System.out.println("  EXIT / QUIT        - Exit the client REPL");
    }

    private void showNodeStatus(int port) {
        try {
            RpcMessage req = new RpcMessage();
            req.setType("STATUS");
            RpcMessage resp = RpcClient.send(port, req, 500);

            System.out.println("\n--- Node " + port + " Status ---");
            System.out.println("State:         " + resp.getState());
            System.out.println("Term:          " + resp.getTerm());
            System.out.println("Leader ID:     " + (resp.getLeaderId() == null ? "None" : resp.getLeaderId()));
            System.out.println("Commit Index:  " + resp.getLeaderCommit());
            System.out.println("Last Applied:  " + resp.getLastLogIndex());
            System.out.println("State Machine: " + resp.getStateMachine());
            System.out.println("WAL Log size:  " + (resp.getLogEntries() != null ? resp.getLogEntries().size() : 0));
            if (resp.getLogEntries() != null && !resp.getLogEntries().isEmpty()) {
                System.out.println("WAL Entries:");
                for (var entry : resp.getLogEntries()) {
                    System.out.println("  " + entry.getIndex() + " [Term " + entry.getTerm() + "]: " + entry.getCommand());
                }
            }
            System.out.println();
        } catch (Exception e) {
            System.out.println("Could not connect to node on port " + port + ": Offline");
        }
    }

    private void showClusterStatus() {
        System.out.println("\n================ CLUSTER STATUS ================");
        for (int p : clusterPorts) {
            try {
                RpcMessage req = new RpcMessage();
                req.setType("STATUS");
                RpcMessage resp = RpcClient.send(p, req, 150);
                String lead = (resp.getLeaderId() != null && resp.getLeaderId() == p) ? "★ LEADER" : "  " + resp.getState();
                System.out.printf("Port %d | %-10s | Term: %d | CommitIndex: %d | Keys: %s%n",
                        p, lead, resp.getTerm(), resp.getLeaderCommit(), resp.getStateMachine().keySet());
            } catch (Exception e) {
                System.out.printf("Port %d | Offline%n", p);
            }
        }
        System.out.println("================================================\n");
    }

    private void executeCommandWithRedirects(String commandLine) {
        int retries = 5;
        int targetPort = currentPort;

        for (int i = 0; i < retries; i++) {
            try {
                RpcMessage req = new RpcMessage();
                req.setType("CLIENT_COMMAND");
                req.setCommand(commandLine);

                RpcMessage resp = RpcClient.send(targetPort, req, 2500);

                if ("OK".equals(resp.getStatus())) {
                    if (resp.getValue() != null) {
                        System.out.println(resp.getValue());
                    } else {
                        System.out.println("OK (" + resp.getMessage() + ")");
                    }
                    currentPort = targetPort; // Stay connected to the successful node
                    return;
                } else if ("REDIRECT".equals(resp.getStatus())) {
                    Integer leaderPort = resp.getLeaderPort();
                    if (leaderPort == null) {
                        System.out.println("Leader is currently unknown. Retrying in 300ms...");
                        Thread.sleep(300);
                    } else {
                        System.out.println("Redirecting to leader on port " + leaderPort + "...");
                        targetPort = leaderPort;
                    }
                } else {
                    System.err.println("ERROR: " + resp.getMessage());
                    return;
                }
            } catch (Exception e) {
                System.out.println("Failed to communicate with node on port " + targetPort + ": " + e.getMessage());
                // If communication fails, find another online cluster node to connect to
                System.out.println("Attempting to failover connection to other cluster nodes...");
                boolean foundNewPort = false;
                for (int port : clusterPorts) {
                    if (port != targetPort) {
                        try {
                            RpcMessage ping = new RpcMessage();
                            ping.setType("STATUS");
                            RpcClient.send(port, ping, 100);
                            targetPort = port;
                            foundNewPort = true;
                            System.out.println("Switched client target port to " + port);
                            break;
                        } catch (Exception ignored) {
                        }
                    }
                }
                if (!foundNewPort) {
                    System.err.println("ERROR: All cluster nodes appear to be offline.");
                    return;
                }
            }
        }
        System.err.println("ERROR: Command timed out or exceeded redirect limits.");
    }

    public static void main(String[] args) {
        int initialPort = 8001;
        if (args.length > 0) {
            initialPort = Integer.parseInt(args[0]);
        }

        List<Integer> clusterPorts;
        if (args.length > 1) {
            clusterPorts = new ArrayList<>();
            for (String p : args[1].split(",")) {
                clusterPorts.add(Integer.parseInt(p.trim()));
            }
        } else {
            clusterPorts = List.of(8001, 8002, 8003, 8004, 8005);
        }
        new KVMemberClient(initialPort, clusterPorts).run();
    }
}
