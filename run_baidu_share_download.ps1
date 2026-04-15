param(
    [Parameter(Mandatory = $true)]
    [string]$ShareUrl,

    [string]$TargetFileName = "",
    [string]$OutputDir = ""
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleDir = Join-Path $root 'modules\baidu_share_downloader'
$scriptPath = Join-Path $moduleDir 'baidu_share_downloader.py'
$defaultOutputDir = Join-Path $root 'runtime\baidu_downloads'

if (-not $OutputDir) {
    $OutputDir = $defaultOutputDir
}

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "Python was not found in PATH."
}

$arguments = @(
    $scriptPath,
    $ShareUrl,
    '--output-dir',
    $OutputDir
)

if ($TargetFileName) {
    $arguments += @('--target-filename', $TargetFileName)
}

Push-Location $moduleDir
try {
    & $pythonCmd.Source @arguments
} finally {
    Pop-Location
}
