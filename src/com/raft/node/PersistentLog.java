package com.raft.node;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PersistentLog {
    private final int port;
    private final File dataDir;
    private final File stateFile;
    private final File walFile;
    private final ObjectMapper mapper = new ObjectMapper();

    private long currentTerm = 0;
    private Integer votedFor = null;
    private final List<LogEntry> entries = new ArrayList<>();

    public PersistentLog(int port) {
        this.port = port;
        this.dataDir = new File("data/node_" + port);
        this.stateFile = new File(dataDir, "state.properties");
        this.walFile = new File(dataDir, "wal.log");

        try {
            if (!dataDir.exists()) {
                Files.createDirectories(dataDir.toPath());
            }
            loadState();
            loadWal();
        } catch (IOException e) {
            throw new RuntimeException("[" + port + "] Failed to initialize storage: ", e);
        }
    }

    private synchronized void loadState() throws IOException {
        if (stateFile.exists()) {
            Properties props = new Properties();
            try (FileInputStream in = new FileInputStream(stateFile)) {
                props.load(in);
            }
            currentTerm = Long.parseLong(props.getProperty("currentTerm", "0"));
            String vf = props.getProperty("votedFor", "");
            votedFor = vf.isEmpty() ? null : Integer.parseInt(vf);
        } else {
            saveStateInternal();
        }
    }

    private synchronized void saveStateInternal() throws IOException {
        Properties props = new Properties();
        props.setProperty("currentTerm", String.valueOf(currentTerm));
        props.setProperty("votedFor", votedFor == null ? "" : String.valueOf(votedFor));
        File tmpFile = new File(dataDir, "state.properties.tmp");
        try (FileOutputStream out = new FileOutputStream(tmpFile)) {
            props.store(out, "Raft Persistent State");
            out.getFD().sync();
        }
        Files.move(tmpFile.toPath(), stateFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private synchronized void loadWal() throws IOException {
        entries.clear();
        if (walFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(walFile))) {
                String line;
                int lineNum = 0;
                while ((line = reader.readLine()) != null) {
                    lineNum++;
                    if (line.trim().isEmpty()) continue;
                    try {
                        LogEntry entry = mapper.readValue(line, LogEntry.class);
                        entries.add(entry);
                    } catch (JsonProcessingException e) {
                        // Malformed trailing record (e.g. partial write after crash).
                        // Stop here; all preceding valid entries are loaded.
                        System.err.println("[" + port + "] WAL line " + lineNum + " malformed, truncating remainder: " + e.getMessage());
                        break;
                    }
                }
            }
        }
    }

    public synchronized long getCurrentTerm() {
        return currentTerm;
    }

    public synchronized Integer getVotedFor() {
        return votedFor;
    }

    public synchronized List<LogEntry> getEntries() {
        return new ArrayList<>(entries);
    }

    public synchronized void saveState(long term, Integer votedFor) {
        this.currentTerm = term;
        this.votedFor = votedFor;
        try {
            saveStateInternal();
        } catch (IOException e) {
            System.err.println("[" + port + "] Failed to persist state: " + e.getMessage());
        }
    }

    public synchronized void appendEntry(LogEntry entry) {
        entries.add(entry);
        try (FileOutputStream fos = new FileOutputStream(walFile, true);
             PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fos)))) {
            out.println(mapper.writeValueAsString(entry));
            out.flush();
            fos.getFD().sync(); // Ensure physical flush for durability
        } catch (IOException e) {
            System.err.println("[" + port + "] Failed to append log entry: " + e.getMessage());
        }
    }

    /**
     * Truncates the log keeping entries from index 1 up to fromIndex - 1.
     * All entries at index >= fromIndex are deleted.
     */
    public synchronized void truncate(long fromIndex) {
        if (fromIndex <= 0) return;
        
        boolean changed = false;
        // Since log indices are 1-based, we remove any entry whose index is >= fromIndex
        while (!entries.isEmpty() && entries.get(entries.size() - 1).getIndex() >= fromIndex) {
            entries.remove(entries.size() - 1);
            changed = true;
        }

        if (changed) {
            try {
                rewriteLog();
            } catch (IOException e) {
                System.err.println("[" + port + "] Failed to rewrite log after truncation: " + e.getMessage());
            }
        }
    }

    private synchronized void rewriteLog() throws IOException {
        File tmpFile = new File(dataDir, "wal.log.tmp");
        try (FileOutputStream fos = new FileOutputStream(tmpFile, false);
             PrintWriter out = new PrintWriter(new BufferedWriter(new OutputStreamWriter(fos)))) {
            for (LogEntry entry : entries) {
                out.println(mapper.writeValueAsString(entry));
            }
            out.flush();
            fos.getFD().sync();
        }
        Files.move(tmpFile.toPath(), walFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    public synchronized long getLastLogIndex() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getIndex();
    }

    public synchronized long getLastLogTerm() {
        if (entries.isEmpty()) return 0;
        return entries.get(entries.size() - 1).getTerm();
    }

    /**
     * Returns the entry at the specified 1-based index, or null if not found.
     */
    public synchronized LogEntry getEntry(long index) {
        if (index <= 0 || index > entries.size()) return null;
        return entries.get((int) index - 1);
    }
}
