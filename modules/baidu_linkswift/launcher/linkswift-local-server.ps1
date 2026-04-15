param(
    [int]$Port = 18911,
    [int]$LifetimeSeconds = 600
)

$ErrorActionPreference = 'Stop'

$launcherDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Split-Path -Parent $launcherDir
$userScriptFiles = Get-ChildItem -LiteralPath $rootDir -Filter '*.user.js' | Sort-Object Length -Descending
$mainScriptFile = $userScriptFiles | Select-Object -First 1
$optionalScriptFile = $userScriptFiles | Select-Object -Skip 1 -First 1

if (-not $mainScriptFile) {
    throw "Missing main userscript in: $rootDir"
}

$mainScriptPath = $mainScriptFile.FullName
$optionalScriptPath = if ($optionalScriptFile) { $optionalScriptFile.FullName } else { $null }

function Write-BytesResponse {
    param(
        [Parameter(Mandatory = $true)]$Context,
        [Parameter(Mandatory = $true)][byte[]]$Bytes,
        [Parameter(Mandatory = $true)][string]$ContentType,
        [int]$StatusCode = 200
    )

    $Context.Response.StatusCode = $StatusCode
    $Context.Response.ContentType = $ContentType
    $Context.Response.ContentLength64 = $Bytes.Length
    $Context.Response.OutputStream.Write($Bytes, 0, $Bytes.Length)
}

function Write-TextResponse {
    param(
        [Parameter(Mandatory = $true)]$Context,
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$ContentType,
        [int]$StatusCode = 200
    )

    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    Write-BytesResponse -Context $Context -Bytes $bytes -ContentType $ContentType -StatusCode $StatusCode
}

function Get-InstallPageHtml {
    param(
        [Parameter(Mandatory = $true)][string]$MainFileName,
        [string]$OptionalFileName
    )

    $mainUrl = '/' + [System.Uri]::EscapeDataString($MainFileName)
    $optionalSection = ''

    if ($OptionalFileName) {
        $optionalUrl = '/' + [System.Uri]::EscapeDataString($OptionalFileName)
        $optionalSection = @"
    <div class="card">
      <h2>Optional Script</h2>
      <p>You can also install the optional Baidu youth-member helper if you want those extra UI tweaks.</p>
      <a class="button secondary" href="$optionalUrl">Install Optional Script</a>
    </div>
"@
    }

    return @"
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LinkSwift Install Page</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f5efe4;
      --card: rgba(255, 255, 255, 0.92);
      --text: #2a2621;
      --muted: #6e6559;
      --accent: #0d6e6e;
      --accent-dark: #084b4b;
      --line: rgba(42, 38, 33, 0.12);
      --shadow: 0 20px 50px rgba(47, 36, 16, 0.14);
    }

    * { box-sizing: border-box; }

    body {
      margin: 0;
      font-family: "Segoe UI", "Microsoft YaHei UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(circle at top left, rgba(255, 220, 163, 0.9), transparent 32%),
        radial-gradient(circle at bottom right, rgba(120, 185, 168, 0.75), transparent 26%),
        linear-gradient(160deg, #f5efe4 0%, #efe5d0 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 28px;
    }

    .shell {
      width: min(860px, 100%);
      background: var(--card);
      border: 1px solid var(--line);
      border-radius: 24px;
      box-shadow: var(--shadow);
      overflow: hidden;
      backdrop-filter: blur(10px);
    }

    .hero {
      padding: 28px 28px 20px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(135deg, rgba(13, 110, 110, 0.08), rgba(255, 206, 129, 0.18));
    }

    .hero h1 {
      margin: 0 0 10px;
      font-size: clamp(28px, 5vw, 42px);
      line-height: 1.05;
    }

    .hero p {
      margin: 0;
      font-size: 15px;
      color: var(--muted);
      line-height: 1.6;
    }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(260px, 1fr));
      gap: 18px;
      padding: 24px 28px 28px;
    }

    .card {
      background: rgba(255, 255, 255, 0.8);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 18px;
    }

    .card h2 {
      margin: 0 0 10px;
      font-size: 18px;
    }

    .card p, .steps li, .footnote {
      font-size: 14px;
      line-height: 1.65;
      color: var(--muted);
    }

    .button {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 180px;
      margin-top: 10px;
      padding: 12px 18px;
      border-radius: 999px;
      text-decoration: none;
      font-weight: 700;
      transition: transform .15s ease, box-shadow .15s ease, background .15s ease;
    }

    .button:hover {
      transform: translateY(-1px);
    }

    .primary {
      color: #fff;
      background: linear-gradient(135deg, var(--accent), var(--accent-dark));
      box-shadow: 0 10px 24px rgba(13, 110, 110, 0.24);
    }

    .secondary {
      color: var(--text);
      background: rgba(13, 110, 110, 0.08);
      border: 1px solid rgba(13, 110, 110, 0.14);
    }

    .steps {
      margin: 0;
      padding-left: 18px;
    }

    .steps li + li {
      margin-top: 8px;
    }

    .footnote {
      padding: 0 28px 26px;
    }
  </style>
</head>
<body>
  <main class="shell">
    <section class="hero">
      <h1>LinkSwift Is Ready</h1>
      <p>If Tampermonkey did not open the install page automatically, use the button below. After installation, open Baidu Netdisk and the script will run inside that page.</p>
    </section>
    <section class="grid">
      <div class="card">
        <h2>Main Script</h2>
        <p>This is the core LinkSwift script. It adds download helper actions inside Baidu Netdisk pages.</p>
        <a class="button primary" href="$mainUrl">Install Main Script</a>
        <a class="button secondary" href="https://pan.baidu.com/disk/home" target="_blank" rel="noreferrer">Open Baidu Netdisk</a>
      </div>
      <div class="card">
        <h2>How To Use</h2>
        <ol class="steps">
          <li>In the Tampermonkey tab, click Install once.</li>
          <li>Open Baidu Netdisk and sign in.</li>
          <li>Open a file page or share page and look for the LinkSwift download helper buttons.</li>
        </ol>
      </div>
$optionalSection
    </section>
    <p class="footnote">This page is served by a temporary local service and does not stay running in the background. After the first installation, use the desktop shortcut for daily startup.</p>
  </main>
</body>
</html>
"@
}

$knownFiles = @{}
$knownFiles[[System.IO.Path]::GetFileName($mainScriptPath)] = $mainScriptPath

if ($optionalScriptPath -and (Test-Path -LiteralPath $optionalScriptPath)) {
    $knownFiles[[System.IO.Path]::GetFileName($optionalScriptPath)] = $optionalScriptPath
}

$listener = [System.Net.HttpListener]::new()
$listener.Prefixes.Add("http://127.0.0.1:$Port/")
$listener.Start()

try {
    $deadline = (Get-Date).AddSeconds($LifetimeSeconds)
    $pendingContext = $listener.GetContextAsync()

    while ((Get-Date) -lt $deadline) {
        if (-not $pendingContext.AsyncWaitHandle.WaitOne(500)) {
            continue
        }

        try {
            $context = $pendingContext.GetAwaiter().GetResult()
        } catch {
            break
        }

        $pendingContext = $listener.GetContextAsync()

        try {
            $path = [System.Uri]::UnescapeDataString($context.Request.Url.AbsolutePath.TrimStart('/'))

            if ([string]::IsNullOrWhiteSpace($path) -or $path -eq 'install.html') {
                $optionalFileName = if ($optionalScriptPath -and (Test-Path -LiteralPath $optionalScriptPath)) {
                    [System.IO.Path]::GetFileName($optionalScriptPath)
                } else {
                    $null
                }

                $html = Get-InstallPageHtml `
                    -MainFileName ([System.IO.Path]::GetFileName($mainScriptPath)) `
                    -OptionalFileName $optionalFileName
                Write-TextResponse -Context $context -Text $html -ContentType 'text/html; charset=utf-8'
                continue
            }

            if ($knownFiles.ContainsKey($path)) {
                $bytes = [System.IO.File]::ReadAllBytes($knownFiles[$path])
                Write-BytesResponse -Context $context -Bytes $bytes -ContentType 'application/javascript; charset=utf-8'
                continue
            }

            Write-TextResponse -Context $context -Text 'Not Found' -ContentType 'text/plain; charset=utf-8' -StatusCode 404
        } catch {
            $errorText = "Server error: $($_.Exception.Message)"
            try {
                Write-TextResponse -Context $context -Text $errorText -ContentType 'text/plain; charset=utf-8' -StatusCode 500
            } catch {
            }
        } finally {
            try { $context.Response.OutputStream.Close() } catch {}
            try { $context.Response.Close() } catch {}
        }
    }
} finally {
    try { $listener.Stop() } catch {}
    try { $listener.Close() } catch {}
}
