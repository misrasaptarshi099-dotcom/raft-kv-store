$libDir = Join-Path $PSScriptRoot "lib"
if (-not (Test-Path $libDir)) {
    New-Item -ItemType Directory -Path $libDir | Out-Null
}

$jacksonVersion = "2.17.2"
$baseUrl = "https://repo1.maven.org/maven2/com/fasterxml/jackson/core"

$jars = @(
    "jackson-core",
    "jackson-annotations",
    "jackson-databind"
)

foreach ($jar in $jars) {
    $fileName = "$jar-$jacksonVersion.jar"
    $dest = Join-Path $libDir $fileName
    $url = "$baseUrl/$jar/$jacksonVersion/$fileName"
    
    if (-not (Test-Path $dest)) {
        Write-Host "Downloading $url to $dest..."
        Invoke-WebRequest -Uri $url -OutFile $dest
    } else {
        Write-Host "$fileName already exists."
    }
}
