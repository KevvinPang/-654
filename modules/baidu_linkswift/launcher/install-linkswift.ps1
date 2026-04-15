param(
    [int]$Port = 18911
)

$ErrorActionPreference = 'Stop'

function Show-ErrorMessage {
    param([string]$Message)

    Add-Type -AssemblyName System.Windows.Forms
    [System.Windows.Forms.MessageBox]::Show(
        $Message,
        'LinkSwift Installer',
        [System.Windows.Forms.MessageBoxButtons]::OK,
        [System.Windows.Forms.MessageBoxIcon]::Error
    ) | Out-Null
}

$launcherDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Split-Path -Parent $launcherDir
$edgePath = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
$serverScript = Join-Path $launcherDir 'linkswift-local-server.ps1'
$stateFile = Join-Path $launcherDir 'linkswift-launcher.state'
$mainScriptFile = Get-ChildItem -LiteralPath $rootDir -Filter '*.user.js' | Sort-Object Length -Descending | Select-Object -First 1
$mainScriptPath = if ($mainScriptFile) { $mainScriptFile.FullName } else { $null }

if (-not (Test-Path -LiteralPath $edgePath)) {
    Show-ErrorMessage "Microsoft Edge was not found:`n$edgePath"
    exit 1
}

if (-not (Test-Path -LiteralPath $serverScript)) {
    Show-ErrorMessage "Missing install service script:`n$serverScript"
    exit 1
}

if (-not $mainScriptPath) {
    Show-ErrorMessage "Missing LinkSwift main userscript."
    exit 1
}

$helpUrl = "http://127.0.0.1:$Port/install.html"
$scriptFileName = [System.IO.Path]::GetFileName($mainScriptPath)
$installUrl = "http://127.0.0.1:$Port/$([System.Uri]::EscapeDataString($scriptFileName))"
$serverReady = $false

try {
    $probe = Invoke-WebRequest -Uri $helpUrl -UseBasicParsing -TimeoutSec 2
    if ($probe.StatusCode -ge 200 -and $probe.StatusCode -lt 400) {
        $serverReady = $true
    }
} catch {
}

if (-not $serverReady) {
    Start-Process -FilePath 'powershell.exe' -WindowStyle Hidden -ArgumentList @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-File', $serverScript,
        '-Port', "$Port",
        '-LifetimeSeconds', '600'
    ) | Out-Null

    for ($i = 0; $i -lt 20; $i++) {
        Start-Sleep -Milliseconds 500

        try {
            $probe = Invoke-WebRequest -Uri $helpUrl -UseBasicParsing -TimeoutSec 2
            if ($probe.StatusCode -ge 200 -and $probe.StatusCode -lt 400) {
                $serverReady = $true
                break
            }
        } catch {
        }
    }
}

if (-not $serverReady) {
    Show-ErrorMessage "Local install service failed to start.`nPlease try again later."
    exit 1
}

Start-Process -FilePath $edgePath -ArgumentList @($installUrl) | Out-Null
Start-Sleep -Seconds 1
Start-Process -FilePath $edgePath -ArgumentList @($helpUrl) | Out-Null

$stateContent = @(
    'installer_launched=1',
    ('timestamp=' + (Get-Date).ToString('s'))
)
[System.IO.File]::WriteAllLines($stateFile, $stateContent, [System.Text.Encoding]::ASCII)
