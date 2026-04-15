$ErrorActionPreference = 'Stop'

function Find-MediaBinary {
    param(
        [Parameter(Mandatory = $true)]
        [string]$BinaryName
    )

    $binaryFileName = if ($BinaryName.EndsWith('.exe')) { $BinaryName } else { "$BinaryName.exe" }

    $command = Get-Command $BinaryName -ErrorAction SilentlyContinue
    if ($command -and $command.Source) {
        return $command.Source
    }

    $localAppData = $env:LOCALAPPDATA
    if ($localAppData) {
        $wingetLink = Join-Path $localAppData "Microsoft\\WinGet\\Links\\$binaryFileName"
        if (Test-Path -LiteralPath $wingetLink) {
            return $wingetLink
        }

        $packagesRoot = Join-Path $localAppData 'Microsoft\\WinGet\\Packages'
        if (Test-Path -LiteralPath $packagesRoot) {
            $packageDir = Get-ChildItem -LiteralPath $packagesRoot -Directory -ErrorAction SilentlyContinue |
                Where-Object { $_.Name -like 'Gyan.FFmpeg*' -or $_.Name -like 'BtbN.FFmpeg*' -or $_.Name -like 'yt-dlp.FFmpeg*' } |
                Sort-Object Name -Descending |
                Select-Object -First 1
            if ($packageDir) {
                $binary = Get-ChildItem -LiteralPath $packageDir.FullName -Recurse -Filter $binaryFileName -ErrorAction SilentlyContinue |
                    Select-Object -First 1 -ExpandProperty FullName
                if ($binary) {
                    return $binary
                }
            }
        }
    }

    return $null
}

$ffmpegPath = Find-MediaBinary -BinaryName 'ffmpeg'
$ffprobePath = Find-MediaBinary -BinaryName 'ffprobe'

if (-not $ffmpegPath -or -not $ffprobePath) {
    $winget = Get-Command winget -ErrorAction SilentlyContinue
    if (-not $winget) {
        throw 'winget was not found. Please install FFmpeg manually and rerun this script.'
    }

    & $winget.Source install --id Gyan.FFmpeg.Essentials -e --accept-package-agreements --accept-source-agreements
    if ($LASTEXITCODE -ne 0) {
        throw 'Failed to install FFmpeg by using winget.'
    }

    $ffmpegPath = Find-MediaBinary -BinaryName 'ffmpeg'
    $ffprobePath = Find-MediaBinary -BinaryName 'ffprobe'
}

if (-not $ffmpegPath -or -not $ffprobePath) {
    throw 'FFmpeg was installed, but ffmpeg.exe / ffprobe.exe could not be located.'
}

$ffmpegBin = Split-Path -Parent $ffmpegPath
if ($env:PATH -notlike "*$ffmpegBin*") {
    $env:PATH = "$ffmpegBin;$env:PATH"
}

$env:SERVER_AUTO_CLIP_FFMPEG = $ffmpegPath
$env:SERVER_AUTO_CLIP_FFPROBE = $ffprobePath
[Environment]::SetEnvironmentVariable('SERVER_AUTO_CLIP_FFMPEG', $ffmpegPath, 'User')
[Environment]::SetEnvironmentVariable('SERVER_AUTO_CLIP_FFPROBE', $ffprobePath, 'User')

Write-Host 'FFmpeg environment is ready.'
Write-Host 'ffmpeg:' $ffmpegPath
Write-Host 'ffprobe:' $ffprobePath
