$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$launcher = Join-Path $root 'modules\baidu_linkswift\launcher\start-linkswift-baidu.ps1'

if (-not (Test-Path -LiteralPath $launcher)) {
    throw "Launcher not found: $launcher"
}

& powershell.exe -NoProfile -ExecutionPolicy Bypass -File $launcher
