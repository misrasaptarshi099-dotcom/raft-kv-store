package com.raft.rpc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.raft.node.LogEntry;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RpcMessage {
    // Message Type: REQUEST_VOTE, REQUEST_VOTE_RESP, APPEND_ENTRIES, APPEND_ENTRIES_RESP, CLIENT_COMMAND, CLIENT_RESP, STATUS, STATUS_RESP
    private String type;

    // RequestVote Request & Response
    private Long term;
    private Integer candidateId;
    private Long lastLogIndex;
    private Long lastLogTerm;
    private Boolean voteGranted;

    // AppendEntries Request & Response
    private Integer leaderId;
    private Long prevLogIndex;
    private Long prevLogTerm;
    private List<LogEntry> entries;
    private Long leaderCommit;
    private Boolean success;
    private Long matchIndex;
    private Long conflictTerm;   // Term of the conflicting entry (for fast backtracking)
    private Long conflictIndex;  // First index of conflictTerm (for fast backtracking)

    // Client Request & Response
    private String command; // e.g., "PUT key value", "GET key", "DELETE key"
    private String status;  // e.g., "OK", "REDIRECT", "ERROR"
    private Integer leaderPort;
    private String value;
    private String message;

    // Status Check Response
    private String state;
    private Map<String, String> stateMachine;
    private List<LogEntry> logEntries;

    // Default Constructor for Jackson
    public RpcMessage() {}

    // Getters and Setters
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public Long getTerm() { return term; }
    public void setTerm(Long term) { this.term = term; }

    public Integer getCandidateId() { return candidateId; }
    public void setCandidateId(Integer candidateId) { this.candidateId = candidateId; }

    public Long getLastLogIndex() { return lastLogIndex; }
    public void setLastLogIndex(Long lastLogIndex) { this.lastLogIndex = lastLogIndex; }

    public Long getLastLogTerm() { return lastLogTerm; }
    public void setLastLogTerm(Long lastLogTerm) { this.lastLogTerm = lastLogTerm; }

    public Boolean getVoteGranted() { return voteGranted; }
    public void setVoteGranted(Boolean voteGranted) { this.voteGranted = voteGranted; }

    public Integer getLeaderId() { return leaderId; }
    public void setLeaderId(Integer leaderId) { this.leaderId = leaderId; }

    public Long getPrevLogIndex() { return prevLogIndex; }
    public void setPrevLogIndex(Long prevLogIndex) { this.prevLogIndex = prevLogIndex; }

    public Long getPrevLogTerm() { return prevLogTerm; }
    public void setPrevLogTerm(Long prevLogTerm) { this.prevLogTerm = prevLogTerm; }

    public List<LogEntry> getEntries() { return entries; }
    public void setEntries(List<LogEntry> entries) { this.entries = entries; }

    public Long getLeaderCommit() { return leaderCommit; }
    public void setLeaderCommit(Long leaderCommit) { this.leaderCommit = leaderCommit; }

    public Boolean getSuccess() { return success; }
    public void setSuccess(Boolean success) { this.success = success; }

    public Long getMatchIndex() { return matchIndex; }
    public void setMatchIndex(Long matchIndex) { this.matchIndex = matchIndex; }

    public Long getConflictTerm() { return conflictTerm; }
    public void setConflictTerm(Long conflictTerm) { this.conflictTerm = conflictTerm; }

    public Long getConflictIndex() { return conflictIndex; }
    public void setConflictIndex(Long conflictIndex) { this.conflictIndex = conflictIndex; }

    public String getCommand() { return command; }
    public void setCommand(String command) { this.command = command; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Integer getLeaderPort() { return leaderPort; }
    public void setLeaderPort(Integer leaderPort) { this.leaderPort = leaderPort; }

    public String getValue() { return value; }
    public void setValue(String value) { this.value = value; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public Map<String, String> getStateMachine() { return stateMachine; }
    public void setStateMachine(Map<String, String> stateMachine) { this.stateMachine = stateMachine; }

    public List<LogEntry> getLogEntries() { return logEntries; }
    public void setLogEntries(List<LogEntry> logEntries) { this.logEntries = logEntries; }

    @Override
    public String toString() {
        return "RpcMessage{type='" + type + "', term=" + term + ", success=" + success + ", status='" + status + "'}";
    }
}
