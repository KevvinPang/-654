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

function Get-ControlCenterProcesses {
    try {
        $items = Get-CimInstance Win32_Process -Filter "Name='python.exe' OR Name='pythonw.exe'" -ErrorAction Stop
    } catch {
        return @()
    }

    return @(
        $items | Where-Object {
            $_.CommandLine -and
            $_.CommandLine -like '*control_center.py*' -and
            $_.CommandLine -like "*$root*"
        }
    )
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

function Get-ControlCenterStatus {
    $url = "http://${hostName}:$port/api/status"
    try {
        $status = Invoke-RestMethod -Uri $url -TimeoutSec 2 -ErrorAction Stop
    } catch {
        return $null
    }

    if (-not $status.server -or -not $status.server.project_root) {
        return $null
    }

    try {
        $statusRoot = [System.IO.Path]::GetFullPath([string]$status.server.project_root).TrimEnd('\')
        $localRoot = [System.IO.Path]::GetFullPath($root).TrimEnd('\')
    } catch {
        return $null
    }

    if ([string]::Equals($statusRoot, $localRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
        return $status
    }

    return $null
}

function Get-ProcessStartTimeUtc {
    param($ProcessInfo)

    if (-not $ProcessInfo) {
        return $null
    }

    $creationDate = $ProcessInfo.CreationDate
    if ($creationDate) {
        if ($creationDate -is [datetime]) {
            return $creationDate.ToUniversalTime()
        }
        try {
            return [System.Management.ManagementDateTimeConverter]::ToDateTime([string]$creationDate).ToUniversalTime()
        } catch {
        }
    }

    try {
        return (Get-Process -Id ([int]$ProcessInfo.ProcessId) -ErrorAction Stop).StartTime.ToUniversalTime()
    } catch {
        return $null
    }
}

function Stop-ControlCenterProcessIds {
    param([object[]]$ProcessIds)

    foreach ($rawProcessId in @($ProcessIds | Where-Object { $_ } | Select-Object -Unique)) {
        try {
            Stop-Process -Id ([int]$rawProcessId) -Force -ErrorAction SilentlyContinue
        } catch {
        }
    }
}

$existingPid = Read-PidFile
$existingStatus = Get-ControlCenterStatus
if ($existingStatus -and $existingStatus.server.pid) {
    try {
        $existingPid = [int]$existingStatus.server.pid
    } catch {
    }
}
$existingProcess = Get-ControlCenterProcess -ProcessId $existingPid
$allExistingProcesses = @(Get-ControlCenterProcesses)
if (-not $existingProcess -and $allExistingProcesses.Count -gt 0) {
    $existingProcess = $allExistingProcesses | Sort-Object ProcessId | Select-Object -First 1
    $existingPid = [int]$existingProcess.ProcessId
}
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
$existingProcessStartTime = Get-ProcessStartTimeUtc $existingProcess
$needsRestart = $false

if (
    $existingProcess -and
    (
        ($existingProcessStartTime -and $latestCodeWriteTime -and $latestCodeWriteTime -gt $existingProcessStartTime) -or
        ((-not $existingProcessStartTime) -and ((-not $pidWriteTime) -or ($latestCodeWriteTime -and $latestCodeWriteTime -gt $pidWriteTime)))
    )
) {
    try {
        $idsToStop = @()
        if ($existingPid) {
            $idsToStop += $existingPid
        }
        if ($existingStatus -and $existingStatus.server.pid) {
            $idsToStop += $existingStatus.server.pid
        }
        $idsToStop += @($allExistingProcesses | ForEach-Object { $_.ProcessId })
        Stop-ControlCenterProcessIds $idsToStop
        Start-Sleep -Milliseconds 600
    } catch {
    }
    $existingStatus = Get-ControlCenterStatus
    if ($existingStatus -and $existingStatus.server.pid) {
        try {
            $existingPid = [int]$existingStatus.server.pid
        } catch {
        }
    }
    $existingProcess = Get-ControlCenterProcess -ProcessId $existingPid
    $needsRestart = $true
}

if ($existingProcess -and -not $needsRestart) {
    foreach ($item in $allExistingProcesses) {
        if ($item.ProcessId -ne $existingPid) {
            Stop-Process -Id $item.ProcessId -Force -ErrorAction SilentlyContinue
        }
    }
    Set-Content -LiteralPath $pidFile -Value $existingPid -Encoding ASCII
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
    $stdoutText = if (Test-Path -LiteralPath $stdoutLogFile) { Get-Content -LiteralPath $stdoutLogFile -Raw -ErrorAction SilentlyContinue } else { '' }
    if ($stdoutText -match 'CONTROL_CENTER_ALREADY_RUNNING') {
        $existingStatus = Get-ControlCenterStatus
        if ($existingStatus -and $existingStatus.server.pid) {
            Set-Content -LiteralPath $pidFile -Value ([int]$existingStatus.server.pid) -Encoding ASCII
        }
        Start-Process "http://${hostName}:$port"
        exit 0
    }
    throw "Control center failed to start. Check $stdoutLogFile and $stderrLogFile"
}

Start-Process "http://${hostName}:$port"
