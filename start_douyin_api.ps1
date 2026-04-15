$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleDir = Join-Path $root 'modules\douyin_api'
$venvPython = Join-Path $moduleDir '.venv\Scripts\python.exe'

if (Test-Path -LiteralPath $venvPython) {
    $pythonExe = $venvPython
} else {
    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if (-not $pythonCmd) {
        throw "Python was not found in PATH. Run .\\init_douyin_env.ps1 first."
    }
    $pythonExe = $pythonCmd.Source
}

Push-Location $moduleDir
try {
    & $pythonExe 'start.py'
} finally {
    Pop-Location
}
