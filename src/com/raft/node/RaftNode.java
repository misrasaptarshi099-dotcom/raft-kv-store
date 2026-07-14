package com.raft.node;

import com.raft.rpc.RpcClient;
import com.raft.rpc.RpcHandler;
import com.raft.rpc.RpcMessage;
import com.raft.rpc.RpcServer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class RaftNode implements RpcHandler {
    private final int nodeId; // port number
    private final List<Integer> peers; // port numbers of other nodes
    private final PersistentLog storage;
    private final RpcServer server;
    private final ReentrantLock lock = new ReentrantLock();
    private final Logger logger;

    private NodeState state = NodeState.FOLLOWER;
    private Integer leaderId = null;

    // Volatile state on all servers
    private volatile long commitIndex = 0;
    private long lastApplied = 0;

    // Replicated state machine
    private final Map<String, String> stateMachine = new ConcurrentHashMap<>();

    // Leader state (reinitialized after election)
    private final Map<Integer, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<Integer, Long> matchIndex = new ConcurrentHashMap<>();

    // Threading & timers
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private final ExecutorService peerExecutor = new ThreadPoolExecutor(
            4, 16, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(256),
            new ThreadPoolExecutor.CallerRunsPolicy());
    private final Object commitEvent = new Object();

    private long electionTimeoutMs;
    private long lastHeartbeatReceived;
    private ScheduledFuture<?> electionTimerTask;
    private ScheduledFuture<?> heartbeatTimerTask;

    public RaftNode(int port, List<Integer> peers) {
        this.nodeId = port;
        this.peers = peers;
        this.storage = new PersistentLog(port);
        this.server = new RpcServer(port, this);
        this.logger = Logger.getLogger("RaftNode-" + port);

        // Replay existing log entries
        this.commitIndex = storage.getLastLogIndex();
        applyLogs();

        resetElectionTimeout();
        log("Initialized node. Term: " + storage.getCurrentTerm() + ", Last Log Index: " + storage.getLastLogIndex());
    }

    public void start() {
        server.start();
        
        // Start election timer task
        electionTimerTask = scheduler.scheduleAtFixedRate(this::checkElectionTimeout, 50, 50, TimeUnit.MILLISECONDS);
        
        log("Node started on port " + nodeId);
    }

    public void stop() {
        log("Stopping node...");
        server.stop();
        if (electionTimerTask != null) electionTimerTask.cancel(true);
        if (heartbeatTimerTask != null) heartbeatTimerTask.cancel(true);
        scheduler.shutdown();
        peerExecutor.shutdown();
        try {
            scheduler.awaitTermination(1, TimeUnit.SECONDS);
            peerExecutor.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log("Node stopped.");
    }

    // ==========================================
    // State Transitions
    // ==========================================

    /**
     * Transitions to FOLLOWER state. Caller MUST hold the lock.
     */
    private void transitionToFollower(long term, Integer newLeaderId) {
        state = NodeState.FOLLOWER;
        leaderId = newLeaderId;
        storage.saveState(term, null); // votedFor is reset for the new term
        resetElectionTimeout();
        
        if (heartbeatTimerTask != null) {
            heartbeatTimerTask.cancel(true);
            heartbeatTimerTask = null;
        }
        log("Transitioned to FOLLOWER. Term: " + term + ", Leader: " + newLeaderId);
    }

    private void startElection() {
        lock.lock();
        try {
            state = NodeState.CANDIDATE;
            long nextTerm = storage.getCurrentTerm() + 1;
            storage.saveState(nextTerm, nodeId); // Vote for self
            leaderId = null;
            resetElectionTimeout();
            log("Started election. Term: " + nextTerm);

            final long electionTerm = nextTerm;
            final long lastLogIndex = storage.getLastLogIndex();
            final long lastLogTerm = storage.getLastLogTerm();
            
            // Collect votes
            Set<Integer> votesReceived = ConcurrentHashMap.newKeySet();
            votesReceived.add(nodeId); // Vote from self

            for (int peer : peers) {
                peerExecutor.submit(() -> {
                    try {
                        RpcMessage request = new RpcMessage();
                        request.setType("REQUEST_VOTE");
                        request.setTerm(electionTerm);
                        request.setCandidateId(nodeId);
                        request.setLastLogIndex(lastLogIndex);
                        request.setLastLogTerm(lastLogTerm);

                        RpcMessage response = RpcClient.send(peer, request, 200);

                        lock.lock();
                        try {
                            if (state == NodeState.CANDIDATE && storage.getCurrentTerm() == electionTerm) {
                                if (response.getTerm() > electionTerm) {
                                    transitionToFollower(response.getTerm(), null);
                                    return;
                                }
                                if (response.getVoteGranted()) {
                                    votesReceived.add(peer);
                                    int majorityNeeded = (peers.size() + 1) / 2 + 1;
                                    if (votesReceived.size() >= majorityNeeded) {
                                        transitionToLeader();
                                    }
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    } catch (IOException e) {
                        // Peer is offline, which is fine
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    private void transitionToLeader() {
        lock.lock();
        try {
            if (state != NodeState.CANDIDATE) return;
            state = NodeState.LEADER;
            leaderId = nodeId;
            log("Became leader for term " + storage.getCurrentTerm());

            // Initialize leader structures
            long lastLogIndex = storage.getLastLogIndex();
            for (int peer : peers) {
                nextIndex.put(peer, lastLogIndex + 1);
                matchIndex.put(peer, 0L);
            }

            // Cancel election timers
            resetElectionTimeout();

            // Broadcast initial heartbeats immediately
            broadcastHeartbeats();

            // Start heartbeat task (every 100ms)
            if (heartbeatTimerTask != null) {
                heartbeatTimerTask.cancel(true);
            }
            heartbeatTimerTask = scheduler.scheduleAtFixedRate(this::broadcastHeartbeats, 100, 100, TimeUnit.MILLISECONDS);
        } finally {
            lock.unlock();
        }
    }

    // ==========================================
    // Periodic Timers
    // ==========================================

    private void checkElectionTimeout() {
        lock.lock();
        try {
            if (state == NodeState.LEADER) return;
            if (System.currentTimeMillis() - lastHeartbeatReceived > electionTimeoutMs) {
                log("Election timeout expired. No heartbeat received from leader. Initiating election.");
                startElection();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resets the election timeout with a randomized delay. Caller MUST hold the lock.
     */
    private void resetElectionTimeout() {
        this.electionTimeoutMs = ThreadLocalRandom.current().nextLong(300, 600);
        this.lastHeartbeatReceived = System.currentTimeMillis();
    }

    private void broadcastHeartbeats() {
        lock.lock();
        try {
            if (state != NodeState.LEADER) return;

            long currentTerm = storage.getCurrentTerm();
            long leaderCommit = commitIndex;

            for (int peer : peers) {
                long prevLogIndexVal = nextIndex.get(peer) - 1;
                long prevLogTermVal = 0;
                if (prevLogIndexVal > 0) {
                    LogEntry prevEntry = storage.getEntry(prevLogIndexVal);
                    if (prevEntry != null) {
                        prevLogTermVal = prevEntry.getTerm();
                    }
                }

                // Gather entries to replicate (capped at 100 per batch)
                List<LogEntry> entriesToSend = new ArrayList<>();
                long lastLogIdx = storage.getLastLogIndex();
                int maxBatchSize = 100;
                if (lastLogIdx >= nextIndex.get(peer)) {
                    for (long i = nextIndex.get(peer); i <= lastLogIdx && entriesToSend.size() < maxBatchSize; i++) {
                        LogEntry e = storage.getEntry(i);
                        if (e != null) {
                            entriesToSend.add(e);
                        }
                    }
                }

                final int targetPeer = peer;
                final long finalPrevLogIndex = prevLogIndexVal;
                final long finalPrevLogTerm = prevLogTermVal;
                final List<LogEntry> finalEntries = entriesToSend;

                peerExecutor.submit(() -> sendAppendEntries(targetPeer, currentTerm, finalPrevLogIndex, finalPrevLogTerm, finalEntries, leaderCommit));
            }
        } finally {
            lock.unlock();
        }
    }

    private void sendAppendEntries(int peerPort, long term, long prevLogIndex, long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
        try {
            RpcMessage request = new RpcMessage();
            request.setType("APPEND_ENTRIES");
            request.setTerm(term);
            request.setLeaderId(nodeId);
            request.setPrevLogIndex(prevLogIndex);
            request.setPrevLogTerm(prevLogTerm);
            request.setEntries(entries);
            request.setLeaderCommit(leaderCommit);

            RpcMessage response = RpcClient.send(peerPort, request, 200);

            lock.lock();
            try {
                if (state == NodeState.LEADER && storage.getCurrentTerm() == term) {
                    if (response.getTerm() > term) {
                        transitionToFollower(response.getTerm(), null);
                        return;
                    }
                    if (response.getSuccess()) {
                        // Success! Update match and next indexes
                        long lastIdxSent = prevLogIndex + entries.size();
                        matchIndex.put(peerPort, Math.max(matchIndex.get(peerPort), lastIdxSent));
                        nextIndex.put(peerPort, matchIndex.get(peerPort) + 1);

                        // Check if we can commit
                        checkAndAdvanceCommitIndex();
                    } else {
                        // Log inconsistency — use conflict hints for fast backtracking
                        long newNextIdx;
                        if (response.getConflictTerm() != null && response.getConflictTerm() > 0) {
                            // Search leader's log for the last entry of conflictTerm
                            long leaderLastOfTerm = -1;
                            for (long i = storage.getLastLogIndex(); i >= 1; i--) {
                                LogEntry e = storage.getEntry(i);
                                if (e != null && e.getTerm() == response.getConflictTerm()) {
                                    leaderLastOfTerm = i;
                                    break;
                                }
                            }
                            if (leaderLastOfTerm >= 0) {
                                // Leader has entries from conflictTerm — set nextIndex to one past it
                                newNextIdx = leaderLastOfTerm + 1;
                            } else {
                                // Leader doesn't have conflictTerm — skip to conflictIndex
                                newNextIdx = response.getConflictIndex() != null ? response.getConflictIndex() : 1;
                            }
                        } else if (response.getConflictIndex() != null) {
                            newNextIdx = response.getConflictIndex();
                        } else {
                            // Fallback: decrement by 1
                            newNextIdx = Math.max(1L, nextIndex.get(peerPort) - 1);
                        }
                        nextIndex.put(peerPort, newNextIdx);
                        
                        // Retry immediately to catch them up faster
                        long newPrevLogIdx = newNextIdx - 1;
                        long newPrevLogTerm = 0;
                        if (newPrevLogIdx > 0) {
                            LogEntry prevEntry = storage.getEntry(newPrevLogIdx);
                            if (prevEntry != null) newPrevLogTerm = prevEntry.getTerm();
                        }
                        List<LogEntry> newEntries = new ArrayList<>();
                        int batchCap = 100;
                        for (long i = newNextIdx; i <= storage.getLastLogIndex() && newEntries.size() < batchCap; i++) {
                            LogEntry e = storage.getEntry(i);
                            if (e != null) newEntries.add(e);
                        }
                        
                        final long retryPrevIdx = newPrevLogIdx;
                        final long retryPrevTerm = newPrevLogTerm;
                        peerExecutor.submit(() -> sendAppendEntries(peerPort, term, retryPrevIdx, retryPrevTerm, newEntries, leaderCommit));
                    }
                }
            } finally {
                lock.unlock();
            }
        } catch (IOException e) {
            // Peer is offline
        }
    }

    private void checkAndAdvanceCommitIndex() {
        lock.lock();
        try {
            long lastLogIdx = storage.getLastLogIndex();
            // Try to find a majority commit point N > commitIndex
            for (long n = lastLogIdx; n > commitIndex; n--) {
                LogEntry entry = storage.getEntry(n);
                if (entry != null && entry.getTerm() == storage.getCurrentTerm()) {
                    int count = 1; // Count leader itself
                    for (int peer : peers) {
                        if (matchIndex.get(peer) >= n) {
                            count++;
                        }
                    }
                    int majorityNeeded = (peers.size() + 1) / 2 + 1;
                    if (count >= majorityNeeded) {
                        commitIndex = n;
                        applyLogs();
                        synchronized (commitEvent) {
                            commitEvent.notifyAll();
                        }
                        break;
                    }
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void applyLogs() {
        lock.lock();
        try {
            while (commitIndex > lastApplied) {
                lastApplied++;
                LogEntry entry = storage.getEntry(lastApplied);
                if (entry != null && entry.getCommand() != null) {
                    executeCommand(entry.getCommand());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void executeCommand(String commandStr) {
        String[] parts = commandStr.split(":", 3);
        if (parts.length < 2) return;
        String type = parts[0];
        String key = parts[1];
        if ("PUT".equals(type) && parts.length == 3) {
            stateMachine.put(key, parts[2]);
        } else if ("DELETE".equals(type)) {
            stateMachine.remove(key);
        }
    }
    // ==========================================
    // ReadIndex: Leadership Confirmation
    // ==========================================

    /**
     * Confirms this node is still the leader by sending heartbeats and
     * waiting for a majority of peers to acknowledge. Returns true if
     * a quorum responds within the timeout.
     */
    private boolean confirmLeadership(long expectedTerm) {
        int majorityNeeded = (peers.size() + 1) / 2 + 1;
        // Leader counts itself
        java.util.concurrent.atomic.AtomicInteger acks = new java.util.concurrent.atomic.AtomicInteger(1);
        CountDownLatch latch = new CountDownLatch(majorityNeeded - 1);

        for (int peer : peers) {
            peerExecutor.submit(() -> {
                try {
                    RpcMessage heartbeat = new RpcMessage();
                    heartbeat.setType("APPEND_ENTRIES");
                    heartbeat.setTerm(expectedTerm);
                    heartbeat.setLeaderId(nodeId);
                    heartbeat.setPrevLogIndex(0L);
                    heartbeat.setPrevLogTerm(0L);
                    heartbeat.setEntries(null);
                    heartbeat.setLeaderCommit(commitIndex);

                    RpcMessage resp = RpcClient.send(peer, heartbeat, 200);
                    if (resp.getSuccess() != null && resp.getSuccess()
                            && resp.getTerm() != null && resp.getTerm() <= expectedTerm) {
                        acks.incrementAndGet();
                        latch.countDown();
                    }
                } catch (IOException e) {
                    // Peer offline — does not count toward quorum
                }
            });
        }

        try {
            // Wait up to 500ms for majority
            latch.await(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Verify we are still leader and term hasn't changed
        lock.lock();
        try {
            return state == NodeState.LEADER
                    && storage.getCurrentTerm() == expectedTerm
                    && acks.get() >= majorityNeeded;
        } finally {
            lock.unlock();
        }
    }



    @Override
    public RpcMessage handleMessage(RpcMessage message) {
        switch (message.getType()) {
            case "REQUEST_VOTE":
                return handleRequestVote(message);
            case "APPEND_ENTRIES":
                return handleAppendEntries(message);
            case "CLIENT_COMMAND":
                return handleClientCommand(message);
            case "STATUS":
                return handleStatusRequest();
            default:
                RpcMessage resp = new RpcMessage();
                resp.setType("ERROR");
                resp.setMessage("Unknown message type");
                return resp;
        }
    }

    private RpcMessage handleRequestVote(RpcMessage request) {
        lock.lock();
        try {
            RpcMessage response = new RpcMessage();
            response.setType("REQUEST_VOTE_RESP");

            long currentTerm = storage.getCurrentTerm();
            long reqTerm = request.getTerm();

            if (reqTerm < currentTerm) {
                response.setTerm(currentTerm);
                response.setVoteGranted(false);
                return response;
            }

            if (reqTerm > currentTerm) {
                transitionToFollower(reqTerm, null);
                currentTerm = reqTerm;
            }

            Integer votedFor = storage.getVotedFor();
            boolean canVote = (votedFor == null || votedFor.equals(request.getCandidateId()));

            // Up-to-date check
            boolean logUpToDate = false;
            long lastLogTerm = storage.getLastLogTerm();
            long lastLogIdx = storage.getLastLogIndex();

            if (request.getLastLogTerm() > lastLogTerm) {
                logUpToDate = true;
            } else if (request.getLastLogTerm() == lastLogTerm) {
                if (request.getLastLogIndex() >= lastLogIdx) {
                    logUpToDate = true;
                }
            }

            if (canVote && logUpToDate) {
                storage.saveState(currentTerm, request.getCandidateId());
                resetElectionTimeout();
                response.setTerm(currentTerm);
                response.setVoteGranted(true);
                log("Granted vote to Candidate " + request.getCandidateId() + " in term " + currentTerm);
            } else {
                response.setTerm(currentTerm);
                response.setVoteGranted(false);
                log("Refused vote to Candidate " + request.getCandidateId() + " (canVote=" + canVote + ", logUpToDate=" + logUpToDate + ")");
            }

            return response;
        } finally {
            lock.unlock();
        }
    }

    private RpcMessage handleAppendEntries(RpcMessage request) {
        lock.lock();
        try {
            RpcMessage response = new RpcMessage();
            response.setType("APPEND_ENTRIES_RESP");

            long currentTerm = storage.getCurrentTerm();
            long reqTerm = request.getTerm();

            // 1. Reply false if term < currentTerm
            if (reqTerm < currentTerm) {
                response.setTerm(currentTerm);
                response.setSuccess(false);
                return response;
            }

            // If term is newer, update local term and transition to follower
            if (reqTerm > currentTerm || state == NodeState.CANDIDATE) {
                transitionToFollower(reqTerm, request.getLeaderId());
                currentTerm = reqTerm;
            }

            // We are follower, reset timeout and update leader
            resetElectionTimeout();
            leaderId = request.getLeaderId();

            // 2. Reply false if log doesn't contain entry at prevLogIndex matching prevLogTerm
            long prevIdx = request.getPrevLogIndex();
            long prevTerm = request.getPrevLogTerm();

            if (prevIdx > 0) {
                LogEntry entry = storage.getEntry(prevIdx);
                if (entry == null) {
                    // We don't have prevLogIndex at all — hint at our last index
                    response.setTerm(currentTerm);
                    response.setSuccess(false);
                    response.setConflictTerm(0L);
                    response.setConflictIndex(storage.getLastLogIndex() + 1);
                    return response;
                } else if (entry.getTerm() != prevTerm) {
                    // We have an entry but with a different term — find first index of that term
                    long conflictTerm = entry.getTerm();
                    long conflictIdx = prevIdx;
                    for (long i = prevIdx - 1; i >= 1; i--) {
                        LogEntry e = storage.getEntry(i);
                        if (e == null || e.getTerm() != conflictTerm) break;
                        conflictIdx = i;
                    }
                    response.setTerm(currentTerm);
                    response.setSuccess(false);
                    response.setConflictTerm(conflictTerm);
                    response.setConflictIndex(conflictIdx);
                    return response;
                }
            }

            // 3. If an existing entry conflicts with a new one, delete existing entry and all follow it
            List<LogEntry> newEntries = request.getEntries();
            if (newEntries != null && !newEntries.isEmpty()) {
                for (int i = 0; i < newEntries.size(); i++) {
                    LogEntry newEntry = newEntries.get(i);
                    LogEntry existing = storage.getEntry(newEntry.getIndex());
                    if (existing != null && existing.getTerm() == newEntry.getTerm()) {
                        // Terms match — entry already present, skip
                        continue;
                    }
                    // Either a conflict (different term) or a new entry (existing == null)
                    if (existing != null) {
                        // Conflict! Truncate the log from this index onward
                        storage.truncate(newEntry.getIndex());
                    }
                    // Bulk-append this entry and all remaining entries from the leader
                    for (int j = i; j < newEntries.size(); j++) {
                        storage.appendEntry(newEntries.get(j));
                    }
                    break;
                }
            }

            // 4. Update commit index
            if (request.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(request.getLeaderCommit(), storage.getLastLogIndex());
                applyLogs();
            }

            response.setTerm(currentTerm);
            response.setSuccess(true);
            response.setMatchIndex(storage.getLastLogIndex());
            return response;
        } finally {
            lock.unlock();
        }
    }

    private RpcMessage handleClientCommand(RpcMessage request) {
        String clientCmd = request.getCommand();
        if (clientCmd == null) {
            RpcMessage errorResp = new RpcMessage();
            errorResp.setType("CLIENT_RESP");
            errorResp.setStatus("ERROR");
            errorResp.setMessage("Empty client command");
            return errorResp;
        }

        String[] parts = clientCmd.split("\\s+");
        String op = parts[0].toUpperCase();

        if ("GET".equals(op)) {
            if (parts.length < 2) {
                RpcMessage errorResp = new RpcMessage();
                errorResp.setType("CLIENT_RESP");
                errorResp.setStatus("ERROR");
                errorResp.setMessage("Usage: GET key");
                return errorResp;
            }
            String key = parts[1];

            // ReadIndex: confirm we are still the leader before serving reads
            lock.lock();
            long readIndexTerm;
            try {
                if (state != NodeState.LEADER) {
                    RpcMessage redirect = new RpcMessage();
                    redirect.setType("CLIENT_RESP");
                    redirect.setStatus("REDIRECT");
                    redirect.setLeaderPort(leaderId);
                    return redirect;
                }
                readIndexTerm = storage.getCurrentTerm();
            } finally {
                lock.unlock();
            }

            // Confirm leadership by sending heartbeat round to quorum
            if (!confirmLeadership(readIndexTerm)) {
                RpcMessage errorResp = new RpcMessage();
                errorResp.setType("CLIENT_RESP");
                errorResp.setStatus("ERROR");
                errorResp.setMessage("Failed to confirm leadership for linearizable read");
                return errorResp;
            }

            // Leadership confirmed — safe to serve read
            String val = stateMachine.get(key);
            RpcMessage ok = new RpcMessage();
            ok.setType("CLIENT_RESP");
            ok.setStatus("OK");
            ok.setValue(val);
            return ok;
        }

        // Writes (PUT/DELETE)
        lock.lock();
        long targetIndex;
        try {
            if (state != NodeState.LEADER) {
                RpcMessage redirect = new RpcMessage();
                redirect.setType("CLIENT_RESP");
                redirect.setStatus("REDIRECT");
                redirect.setLeaderPort(leaderId);
                return redirect;
            }

            // Reconstruct command for WAL
            String logCommand;
            if ("PUT".equals(op)) {
                if (parts.length < 3) {
                    RpcMessage errorResp = new RpcMessage();
                    errorResp.setType("CLIENT_RESP");
                    errorResp.setStatus("ERROR");
                    errorResp.setMessage("Usage: PUT key value");
                    return errorResp;
                }
                String key = parts[1];
                // Support spaces in value
                String val = clientCmd.substring(clientCmd.indexOf(key) + key.length()).trim();
                logCommand = "PUT:" + key + ":" + val;
            } else if ("DELETE".equals(op)) {
                if (parts.length < 2) {
                    RpcMessage errorResp = new RpcMessage();
                    errorResp.setType("CLIENT_RESP");
                    errorResp.setStatus("ERROR");
                    errorResp.setMessage("Usage: DELETE key");
                    return errorResp;
                }
                logCommand = "DELETE:" + parts[1];
            } else {
                RpcMessage errorResp = new RpcMessage();
                errorResp.setType("CLIENT_RESP");
                errorResp.setStatus("ERROR");
                errorResp.setMessage("Unsupported write operation: " + op);
                return errorResp;
            }

            // Append locally
            long nextIdx = storage.getLastLogIndex() + 1;
            LogEntry entry = new LogEntry(nextIdx, storage.getCurrentTerm(), logCommand);
            storage.appendEntry(entry);
            targetIndex = nextIdx;

            // Trigger parallel replication immediately
            broadcastHeartbeats();
        } finally {
            lock.unlock();
        }

        // Wait for replication consensus
        boolean committed = false;
        long timeoutMs = 2000;
        long start = System.currentTimeMillis();

        synchronized (commitEvent) {
            try {
                while (commitIndex < targetIndex && (System.currentTimeMillis() - start < timeoutMs)) {
                    commitEvent.wait(timeoutMs - (System.currentTimeMillis() - start));
                }
                committed = (commitIndex >= targetIndex);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        RpcMessage response = new RpcMessage();
        response.setType("CLIENT_RESP");
        if (committed) {
            response.setStatus("OK");
            response.setMessage("Committed by leader on port " + nodeId);
        } else {
            response.setStatus("ERROR");
            response.setMessage("Write timed out waiting for replication majority.");
        }
        return response;
    }

    private RpcMessage handleStatusRequest() {
        lock.lock();
        try {
            RpcMessage response = new RpcMessage();
            response.setType("STATUS_RESP");
            response.setState(state.name());
            response.setTerm(storage.getCurrentTerm());
            response.setLeaderCommit(commitIndex);
            response.setLastLogIndex(storage.getLastLogIndex());
            response.setLeaderId(leaderId);
            response.setStateMachine(new HashMap<>(stateMachine));
            response.setLogEntries(storage.getEntries());
            return response;
        } finally {
            lock.unlock();
        }
    }

    private void log(String msg) {
        logger.info(() -> String.format("[%d] %s", nodeId, msg));
    }

    // ==========================================
    // Bootstrap Main
    // ==========================================

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: java RaftNode <port> <peerPort1,peerPort2,...>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        List<Integer> peers = new ArrayList<>();
        if (!args[1].trim().isEmpty()) {
            for (String p : args[1].split(",")) {
                peers.add(Integer.parseInt(p));
            }
        }

        RaftNode node = new RaftNode(port, peers);
        node.start();

        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(node::stop));

        // Keep process alive
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            node.stop();
        }
    }
}
