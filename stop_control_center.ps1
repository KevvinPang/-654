$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$runtimeDir = Join-Path $root 'runtime\control_center'
$pidFile = Join-Path $runtimeDir 'control_center.pid'

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

$controlCenterPid = $null
if (Test-Path -LiteralPath $pidFile) {
    $firstLine = Get-Content -LiteralPath $pidFile -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($firstLine) {
        $raw = $firstLine.Trim()
        try {
            $controlCenterPid = [int]$raw
        } catch {
            $controlCenterPid = $null
        }
    }
}

$processInfo = Get-ControlCenterProcess -ProcessId $controlCenterPid
$allProcesses = @(Get-ControlCenterProcesses)
if (-not $processInfo -and $allProcesses.Count -gt 0) {
    $processInfo = $allProcesses | Sort-Object ProcessId | Select-Object -First 1
}
if (-not $processInfo) {
    if (Test-Path -LiteralPath $pidFile) {
        Remove-Item -LiteralPath $pidFile -Force
    }
    Write-Output 'Control center is not running.'
    exit 0
}

foreach ($item in $allProcesses) {
    Stop-Process -Id $item.ProcessId -Force -ErrorAction SilentlyContinue
}

if (Test-Path -LiteralPath $pidFile) {
    Remove-Item -LiteralPath $pidFile -Force
}

Write-Output 'Control center stopped.'
