$ErrorActionPreference = 'Stop'

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$moduleDir = Join-Path $root 'modules\subtitle_extractor'
$exePath = Join-Path $moduleDir 'VideoSubtitleExtractor.exe'

if (-not (Test-Path -LiteralPath $exePath)) {
    throw "Subtitle extractor executable not found: $exePath"
}

Start-Process -FilePath $exePath -WorkingDirectory $moduleDir
