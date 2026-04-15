$ErrorActionPreference = 'Stop'

function Show-ErrorMessage {
    param(
        [string]$Message,
        [string]$Title = 'LinkSwift Launcher',
        [System.Windows.Forms.MessageBoxIcon]$Icon = [System.Windows.Forms.MessageBoxIcon]::Error
    )

    Add-Type -AssemblyName System.Windows.Forms
    [System.Windows.Forms.MessageBox]::Show(
        $Message,
        $Title,
        [System.Windows.Forms.MessageBoxButtons]::OK,
        $Icon
    ) | Out-Null
}

$launcherDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$edgePath = 'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
$targetUrl = 'https://pan.baidu.com/disk/home'
$installScript = Join-Path $launcherDir 'install-linkswift.ps1'
$stateFile = Join-Path $launcherDir 'linkswift-launcher.state'

if (-not (Test-Path -LiteralPath $edgePath)) {
    Show-ErrorMessage -Message "Microsoft Edge was not found:`n$edgePath"
    exit 1
}

if (-not (Test-Path -LiteralPath $stateFile)) {
    Show-ErrorMessage `
        -Message "LinkSwift has not been initialized yet.`nThe installer will open now. Please click Install once in the Tampermonkey tab, then use this shortcut again." `
        -Title 'LinkSwift First Run' `
        -Icon ([System.Windows.Forms.MessageBoxIcon]::Information)

    if (-not (Test-Path -LiteralPath $installScript)) {
        Show-ErrorMessage -Message "Install script was not found:`n$installScript"
        exit 1
    }

    Start-Process -FilePath 'powershell.exe' -ArgumentList @(
        '-NoProfile',
        '-ExecutionPolicy', 'Bypass',
        '-WindowStyle', 'Hidden',
        '-File', $installScript
    ) | Out-Null
    exit 0
}

Start-Process -FilePath $edgePath -ArgumentList @($targetUrl) | Out-Null
