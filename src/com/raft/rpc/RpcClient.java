package com.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

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

            // Write request (explicit UTF-8)
            PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true);
            String json = mapper.writeValueAsString(request);
            out.println(json);

            // Read response (explicit UTF-8)
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            String responseLine = in.readLine();
            if (responseLine == null) {
                throw new IOException("Received EOF from server at port " + targetPort);
            }
            return mapper.readValue(responseLine, RpcMessage.class);
        }
    }
}
