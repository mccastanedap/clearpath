# Usage: . .\activate_dev.ps1   (dot-source so venv activation persists in this session)

$ErrorActionPreference = 'Stop'

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$venvActivate = Join-Path $scriptDir 'venv\Scripts\Activate.ps1'
$envFile = Join-Path $scriptDir '.env'

if (-not (Test-Path $venvActivate)) {
    throw "venv activation script not found at $venvActivate"
}
. $venvActivate
Write-Host "venv activated"

if (-not (Test-Path $envFile)) {
    Write-Warning ".env not found at $envFile - skipping env var load"
    return
}

$loaded = 0
$lineNumber = 0
foreach ($rawLine in Get-Content -LiteralPath $envFile) {
    $lineNumber++
    $line = $rawLine.Trim()
    if ([string]::IsNullOrWhiteSpace($line)) { continue }
    if ($line.StartsWith('#')) { continue }

    $eqIndex = $line.IndexOf('=')
    if ($eqIndex -lt 1) {
        Write-Warning "Skipping malformed line $lineNumber in .env"
        continue
    }

    $key = $line.Substring(0, $eqIndex).Trim()
    $value = $line.Substring($eqIndex + 1).Trim()

    if ($value.Length -ge 2) {
        $first = $value[0]
        $last = $value[$value.Length - 1]
        if (($first -eq '"' -and $last -eq '"') -or ($first -eq "'" -and $last -eq "'")) {
            $value = $value.Substring(1, $value.Length - 2)
        }
    }

    Set-Item -Path "Env:$key" -Value $value
    $loaded++
}

Write-Host "Loaded $loaded variables from .env"
