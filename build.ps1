$binDir = Join-Path $PSScriptRoot "bin"
if (-not (Test-Path $binDir)) {
    New-Item -ItemType Directory -Path $binDir | Out-Null
}

$libDir = Join-Path $PSScriptRoot "lib"
$jars = Get-ChildItem -Path $libDir -Filter *.jar

$classpath = @()
foreach ($jar in $jars) {
    $classpath += $jar.FullName
}
$classpathStr = $classpath -join ";"

Write-Host "Compiling Java files with classpath: $classpathStr"

$javaFiles = Get-ChildItem -Path (Join-Path $PSScriptRoot "src") -Recurse -Filter *.java | ForEach-Object { $_.FullName }

if ($javaFiles.Count -eq 0) {
    Write-Error "No Java source files found!"
    exit 1
}

javac -cp $classpathStr -d $binDir $javaFiles

if ($LASTEXITCODE -eq 0) {
    Write-Host "Compilation successful!" -ForegroundColor Green
} else {
    Write-Error "Compilation failed!"
    exit 1
}
