package com.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

public class RpcClient {
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Sends an RpcMessage to the target port on localhost and returns the response.
     * Throws IOException if the node is unreachable, times out, or errors.
     */
    public static RpcMessage send(int targetPort, RpcMessage request, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("localhost", targetPort), timeoutMs);
            socket.setSoTimeout(timeoutMs);

            // Write request
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            String json = mapper.writeValueAsString(request);
            out.println(json);

            // Read response
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String responseLine = in.readLine();
            if (responseLine == null) {
                throw new IOException("Received EOF from server at port " + targetPort);
            }
            return mapper.readValue(responseLine, RpcMessage.class);
        }
    }
}
