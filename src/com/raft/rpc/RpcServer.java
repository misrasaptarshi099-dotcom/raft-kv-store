package com.raft.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RpcServer {
    private final int port;
    private final RpcHandler handler;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ExecutorService threadPool = new ThreadPoolExecutor(
            4, 32, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(512),
            new ThreadPoolExecutor.CallerRunsPolicy());
    private ServerSocket serverSocket;
    private volatile boolean running = false;
    private Thread listenerThread;

    public RpcServer(int port, RpcHandler handler) {
        this.port = port;
        this.handler = handler;
    }

    public synchronized void start() {
        if (running) return;
        running = true;
        listenerThread = new Thread(this::listenLoop, "RpcServerListener-" + port);
        listenerThread.start();
    }

    public synchronized void stop() {
        if (!running) return;
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (Exception e) {
            // Ignore
        }
        if (listenerThread != null) {
            listenerThread.interrupt();
        }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(2, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();
        }
    }

    private void listenLoop() {
        try {
            serverSocket = new ServerSocket(port);
            while (running) {
                Socket socket = serverSocket.accept();
                threadPool.submit(() -> handleConnection(socket));
            }
        } catch (Exception e) {
            if (running) {
                System.err.println("[" + port + "] RpcServer listener thread error: " + e.getMessage());
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)
        ) {
            String inputLine = in.readLine();
            if (inputLine != null && !inputLine.trim().isEmpty()) {
                RpcMessage request = mapper.readValue(inputLine, RpcMessage.class);
                RpcMessage response = handler.handleMessage(request);
                String responseJson = mapper.writeValueAsString(response);
                out.println(responseJson);
            }
        } catch (Exception e) {
            // Sockets closing abruptly is expected when killing nodes/clients
        } finally {
            try {
                socket.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }
}
