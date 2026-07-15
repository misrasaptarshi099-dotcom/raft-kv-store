param(
    [string]$Action,
    [int]$Port
)

$ports = 8001..8005

function Get-Peers($port) {
    $peers = @()
    foreach ($p in $ports) {
        if ($p -ne $port) {
            $peers += $p
        }
    }
    return $peers -join ","
}

function Stop-NodeByPort($p) {
    $conn = Get-NetTCPConnection -LocalPort $p -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($conn) {
        $owningPid = $conn.OwningProcess
        Write-Host "Stopping node on port $p (PID: $owningPid)..." -ForegroundColor Yellow
        Stop-Process -Id $owningPid -Force -ErrorAction SilentlyContinue
        # Give Windows a moment to release the port
        Start-Sleep -Milliseconds 200
    } else {
        Write-Host "No active process found on port $p." -ForegroundColor Gray
    }
}

function Start-NodeByPort($p) {
    # Check if port is already in use
    $conn = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    if ($conn) {
        Write-Host "Port $p is already in use (Listening)! Cannot start." -ForegroundColor Red
        return
    }

    $peers = Get-Peers $p
    $logFile = "logs/node_$p.log"
    if (-not (Test-Path "logs")) {
        New-Item -ItemType Directory -Path "logs" | Out-Null
    }

    Write-Host "Starting node on port $p with peers $peers..." -ForegroundColor Green
    
    # Run the JVM in background and redirect output to log file
    # We use Start-Process with redirecting options
    $cmdLine = "java -cp bin;lib/* com.raft.node.RaftNode $p $peers > $logFile 2>&1"
    Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", $cmdLine) -NoNewWindow
}

switch ($Action.ToLower()) {
    "start" {
        # Ensure bin is built
        if (-not (Test-Path "bin")) {
            Write-Host "Compiling first..."
            powershell -ExecutionPolicy Bypass -File .\build.ps1
        }
        foreach ($p in $ports) {
            Start-NodeByPort $p
        }
        Write-Host "Cluster started. Logs are in logs/ directory." -ForegroundColor Green
    }
    "stop" {
        foreach ($p in $ports) {
            Stop-NodeByPort $p
        }
        Write-Host "Cluster stopped." -ForegroundColor Green
    }
    "kill" {
        if ($Port -eq 0) {
            Write-Error "Please specify a port to kill, e.g. .\cluster.ps1 kill -Port 8002"
            exit 1
        }
        Stop-NodeByPort $Port
    }
    "start-node" {
        if ($Port -eq 0) {
            Write-Error "Please specify a port to start, e.g. .\cluster.ps1 start-node -Port 8002"
            exit 1
        }
        Start-NodeByPort $Port
    }
    "clean" {
        Write-Host "Stopping cluster..."
        foreach ($p in $ports) {
            Stop-NodeByPort $p
        }
        Start-Sleep -Seconds 1
        Write-Host "Deleting data and logs directories..." -ForegroundColor Yellow
        if (Test-Path "data") {
            Remove-Item -Path "data" -Recurse -Force
        }
        if (Test-Path "logs") {
            Remove-Item -Path "logs" -Recurse -Force
        }
        Write-Host "Cleaned up." -ForegroundColor Green
    }
    "status" {
        Write-Host "================ NODE PROCESSES =================="
        foreach ($p in $ports) {
            $conn = Get-NetTCPConnection -LocalPort $p -ErrorAction SilentlyContinue | Select-Object -First 1
            if ($conn) {
                Write-Host "Port $p : ONLINE (PID: $($conn.OwningProcess))" -ForegroundColor Green
            } else {
                Write-Host "Port $p : OFFLINE" -ForegroundColor Red
            }
        }
        Write-Host "=================================================="
        Write-Host "To see consensus state, run: java -cp `"bin;lib/*`" com.raft.client.KVMemberClient CLUSTER"
    }
    default {
        Write-Host "Usage: .\cluster.ps1 <start | stop | kill | start-node | status | clean> [-Port <port>]"
    }
}
