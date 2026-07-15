# Raft KV Store

A distributed, fault-tolerant key-value store built in Java, implementing the **Raft consensus algorithm**. Nodes communicate via custom TCP sockets using JSON serialization, maintain a persistent write-ahead log (WAL), and automatically handle leader election, log replication, and network partition recovery.

---

## Quick Start

### 1. Prerequisites
- **JDK 11+**
- **PowerShell 5.1+** (for orchestrating local cluster)

### 2. Build & Launch
```powershell
# Compile the project
.\build.ps1

# Start a 5-node cluster (ports 8001-8005)
.\cluster.ps1 start
```

### 3. Interact via Client REPL
Open a client session to connect to the cluster:
```powershell
java -cp "bin;lib/*" com.raft.client.KVMemberClient
```
Inside the REPL:
```
raft-kv:8001> PUT name Saptarshi
OK (Committed by leader on port 8001)

raft-kv:8001> GET name
Saptarshi

raft-kv:8001> CLUSTER
================ CLUSTER STATUS ================
Port 8001 | ★ LEADER   | Term: 1 | CommitIndex: 1 | Keys: [name]
Port 8002 |   FOLLOWER | Term: 1 | CommitIndex: 1 | Keys: [name]
Port 8003 |   FOLLOWER | Term: 1 | CommitIndex: 1 | Keys: [name]
Port 8004 |   FOLLOWER | Term: 1 | CommitIndex: 1 | Keys: [name]
Port 8005 |   FOLLOWER | Term: 1 | CommitIndex: 1 | Keys: [name]
================================================
```

---

## Features & Implementation Highlights

### 1. Protocol Optimizations
* **Linearizable Reads (ReadIndex):** GET reads confirm leadership with a majority quorum before serving data, preventing stale reads during network partitions.
* **Fast Log Backtracking:** Follower rejections include `conflictTerm` and `conflictIndex` hints, allowing the leader to bypass mismatched terms in single jumps instead of decrementing `nextIndex` one-by-one.
* **Randomized Election Timeouts:** Bounded between 300ms–600ms to minimize split-vote occurrences.

### 2. Durability & Crash Safety
* **Fsync on Append:** Every log entry is explicitly flushed to disk via `FileOutputStream.getFD().sync()` before acknowledgment.
* **Atomic State Rewrites:** Modifying metadata (`state.properties`) or truncating the WAL writes first to a `.tmp` file, performs an `fsync`, and replaces the target using `Files.move(..., ATOMIC_MOVE)` to prevent file corruption during crashes.
* **Fault-Tolerant WAL Loading:** Startup parsing tolerates and stops at a malformed trailing record (partial write), preserving and loading all preceding valid entries.

### 3. Data Integrity
* **JSON-formatted WAL:** Commands are serialized as `{"op":"PUT","key":"k","value":"v"}` to avoid delimiter conflicts with keys containing colons, falling back to legacy colon formats for backward compatibility.

---

## Cluster Management Operations

All nodes store execution logs in `logs/` and persistent state in `data/`.

| PowerShell Command | Description |
|---|---|
| `.\cluster.ps1 start` | Start all 5 cluster nodes (ports 8001–8005) |
| `.\cluster.ps1 stop` | Gracefully terminate all running nodes |
| `.\cluster.ps1 kill -Port <port>` | Hard-kill a specific node (e.g. `8001`) |
| `.\cluster.ps1 start-node -Port <port>` | Restart/Start a specific node |
| `.\cluster.ps1 status` | Display running/offline status of each port |
| `.\cluster.ps1 clean` | Stop cluster and delete all `data/` and `logs/` |

---

## Architecture

```
                    ┌──────────────────────┐
                    │   KVMemberClient     │
                    │   (Interactive REPL) │
                    └──────────┬───────────┘
                               │ TCP/JSON
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
      ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
      │  RaftNode    │ │  RaftNode    │ │  RaftNode    │  ...
      │  :8001       │ │  :8002       │ │  :8003       │
      │              │ │              │ │              │
      │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
      │ │RpcServer │ │ │ │RpcServer │ │ │ │RpcServer │ │
      │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
      │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
      │ │Persistent│ │ │ │Persistent│ │ │ │Persistent│ │
      │ │   Log    │ │ │ │   Log    │ │ │ │   Log    │ │
      │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
      │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │
      │ │  State   │ │ │ │  State   │ │ │ │  State   │ │
      │ │ Machine  │ │ │ │ Machine  │ │ │ │ Machine  │ │
      │ └──────────┘ │ │ └──────────┘ │ │ └──────────┘ │
      └──────────────┘ └──────────────┘ └──────────────┘
              ◄─── AppendEntries / RequestVote RPCs ───►
```

