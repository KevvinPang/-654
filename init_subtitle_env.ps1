$ErrorActionPreference = 'Stop'

function Resolve-PreferredPython {
    $pyCmd = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCmd) {
        foreach ($version in @('-3.11', '-3.12')) {
            $pythonPath = & $pyCmd.Source $version -c "import sys; print(sys.executable)" 2>$null
            if ($LASTEXITCODE -eq 0 -and $pythonPath) {
                return $pythonPath.Trim()
            }
        }
    }

    $pythonCmd = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCmd) {
        return $pythonCmd.Source
    }

    throw "Python 3.11 or 3.12 was not found."
}

function Invoke-Checked {
    param(
        [string]$FilePath,
        [string[]]$Arguments,
        [string]$ErrorMessage
    )

    & $FilePath @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw $ErrorMessage
    }
}

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleDir = Join-Path $root 'modules\subtitle_extractor_source\video-subtitle-extractor-main'
$venvDir = Join-Path $moduleDir '.venv'
$requirements = Join-Path $moduleDir 'requirements.txt'
$selectedPython = Resolve-PreferredPython

if (Test-Path -LiteralPath (Join-Path $venvDir 'Scripts\python.exe')) {
    $venvVersion = & (Join-Path $venvDir 'Scripts\python.exe') -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read the existing subtitle virtual environment version."
    }

    $selectedVersion = & $selectedPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to read the selected Python version."
    }

    if ($venvVersion.Trim() -ne $selectedVersion.Trim()) {
        Remove-Item -LiteralPath $venvDir -Recurse -Force
    }
}

if (-not (Test-Path -LiteralPath $venvDir)) {
    Invoke-Checked -FilePath $selectedPython -Arguments @('-m', 'venv', $venvDir) -ErrorMessage "Failed to create the subtitle virtual environment."
}

$venvPython = Join-Path $venvDir 'Scripts\python.exe'
if (-not (Test-Path -LiteralPath $venvPython)) {
    throw "Virtual environment python was not created successfully: $venvPython"
}

Invoke-Checked -FilePath $venvPython -Arguments @('-m', 'pip', 'install', '--upgrade', 'pip') -ErrorMessage "Failed to upgrade pip for the subtitle environment."
Invoke-Checked -FilePath $venvPython -Arguments @('-m', 'pip', 'install', '-r', $requirements) -ErrorMessage "Failed to install subtitle extractor dependencies."

$pythonVersion = & $venvPython -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ($LASTEXITCODE -ne 0) {
    throw "Failed to confirm the subtitle environment Python version."
}

Write-Host "Subtitle extractor source environment is ready."
Write-Host "Python:" $venvPython
Write-Host "Version:" $pythonVersion.Trim()
Write-Host "If you need a GPU or DirectML build later, install the matching PaddlePaddle variant manually."
