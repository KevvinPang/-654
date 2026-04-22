$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $root 'control_center.py'
$uiPath = Join-Path $root 'control_center_ui.html'
$batchRunnerPath = Join-Path $root 'batch_runner.py'
$autoClipEngineDir = Join-Path $root 'modules\auto_clip_engine'
$runtimeDir = Join-Path $root 'runtime\control_center'
$pidFile = Join-Path $runtimeDir 'control_center.pid'
$stdoutLogFile = Join-Path $runtimeDir 'control_center.out.log'
$stderrLogFile = Join-Path $runtimeDir 'control_center.err.log'
$hostName = '127.0.0.1'
$port = 19081

New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null

function Get-ControlCenterProcess {
    param([int]$ProcessId)

    if (-not $ProcessId) {
        return $null
    }

    try {
        $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop
    } catch {
        return $null
    }

    if ($processInfo.CommandLine -and $processInfo.CommandLine -like '*control_center.py*') {
        return $processInfo
    }

    return $null
}

function Read-PidFile {
    if (-not (Test-Path -LiteralPath $pidFile)) {
        return $null
    }
    $firstLine = Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $firstLine) {
        return $null
    }
    $raw = $firstLine.Trim()
    if (-not $raw) {
        return $null
    }
    try {
        return [int]$raw
    } catch {
        return $null
    }
}

$existingPid = Read-PidFile
$existingProcess = Get-ControlCenterProcess -ProcessId $existingPid
$autoClipEngineFiles = @()
if (Test-Path -LiteralPath $autoClipEngineDir) {
    $autoClipEngineFiles = Get-ChildItem -LiteralPath $autoClipEngineDir -Filter '*.py' -File -Recurse | ForEach-Object { $_.FullName }
}
$watchedFileCandidates = @(
    $scriptPath,
    $uiPath,
    $batchRunnerPath
) + $autoClipEngineFiles
$watchedFiles = $watchedFileCandidates | Where-Object { Test-Path -LiteralPath $_ }
$latestCodeWriteTime = ($watchedFiles | ForEach-Object { (Get-Item -LiteralPath $_).LastWriteTimeUtc } | Sort-Object -Descending | Select-Object -First 1)
$pidWriteTime = if (Test-Path -LiteralPath $pidFile) { (Get-Item -LiteralPath $pidFile).LastWriteTimeUtc } else { $null }
$needsRestart = $false

if ($existingProcess -and $pidWriteTime -and $latestCodeWriteTime -and $latestCodeWriteTime -gt $pidWriteTime) {
    try {
        Stop-Process -Id $existingPid -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 600
    } catch {
    }
    $existingProcess = Get-ControlCenterProcess -ProcessId $existingPid
    $needsRestart = $true
}

if ($existingProcess -and -not $needsRestart) {
    Start-Process "http://${hostName}:$port"
    exit 0
}

if (Test-Path -LiteralPath $pidFile) {
    Remove-Item -LiteralPath $pidFile -Force
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw 'Python was not found in PATH.'
}

$arguments = @(
    '-u',
    $scriptPath,
    '--host',
    $hostName,
    '--port',
    [string]$port
)

$process = Start-Process `
    -FilePath $pythonCmd.Source `
    -ArgumentList $arguments `
    -WorkingDirectory $root `
    -WindowStyle Hidden `
    -PassThru `
    -RedirectStandardOutput $stdoutLogFile `
    -RedirectStandardError $stderrLogFile

Set-Content -LiteralPath $pidFile -Value $process.Id -Encoding ASCII
Start-Sleep -Seconds 2

if ($process.HasExited) {
    throw "Control center failed to start. Check $stdoutLogFile and $stderrLogFile"
}

Start-Process "http://${hostName}:$port"
