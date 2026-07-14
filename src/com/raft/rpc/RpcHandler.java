package com.raft.rpc;

public interface RpcHandler {
    RpcMessage handleMessage(RpcMessage message);
}
