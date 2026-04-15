param(
    [string[]]$Config = @(),
    [string[]]$Workspace = @(),
    [switch]$AllWorkspaces,
    [int]$WorkspaceParallel = 2,
    [int]$GlobalBaiduShare = 1,
    [int]$GlobalDouyinDownload = 3,
    [int]$GlobalSubtitleExtract = 1,
    [int]$GlobalAutoClip = 1
)

$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$scriptPath = Join-Path $root 'batch_runner.py'

$pythonCmd = Get-Command python -ErrorAction SilentlyContinue
if (-not $pythonCmd) {
    throw "Python was not found in PATH."
}

$arguments = @(
    $scriptPath,
    '--workspace-parallel', $WorkspaceParallel,
    '--global-baidu-share', $GlobalBaiduShare,
    '--global-douyin-download', $GlobalDouyinDownload,
    '--global-subtitle-extract', $GlobalSubtitleExtract,
    '--global-auto-clip', $GlobalAutoClip
)

if ($AllWorkspaces -or (($Config.Count -eq 0) -and ($Workspace.Count -eq 0))) {
    $arguments += '--all-workspaces'
}

foreach ($item in $Config) {
    $arguments += @('--config', $item)
}

foreach ($item in $Workspace) {
    $arguments += @('--workspace', $item)
}

& $pythonCmd.Source @arguments
