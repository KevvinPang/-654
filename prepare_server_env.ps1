$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$workspaceRoot = Join-Path $root 'runtime\workspaces'
$runtimeDirs = @(
    'runtime',
    'runtime\workspaces',
    'runtime\subtitle_output',
    'runtime\workspaces\demo_workspace'
)

foreach ($relativePath in $runtimeDirs) {
    New-Item -ItemType Directory -Force -Path (Join-Path $root $relativePath) | Out-Null
}

Write-Host 'Preparing Douyin environment...'
& (Join-Path $root 'init_douyin_env.ps1')

Write-Host 'Preparing subtitle extractor environment...'
& (Join-Path $root 'init_subtitle_env.ps1')

Write-Host 'Preparing FFmpeg environment...'
& (Join-Path $root 'init_ffmpeg_env.ps1')

Write-Host 'Preparing auto clip environment...'
& (Join-Path $root 'init_auto_clip_env.ps1')

Write-Host ''
Write-Host 'Preparation completed.'
Write-Host 'Workspace root:' $workspaceRoot
Write-Host 'Next step: double-click start_control_center.bat'
