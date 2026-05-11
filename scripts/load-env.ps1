# Loads .env from the project root and sets KEY=VALUE pairs as process-scoped env vars.
# Dot-source this script so the env vars persist in the caller's session:
#   . .\scripts\load-env.ps1

$ErrorActionPreference = "Stop"

$envPath = (Resolve-Path (Join-Path $PSScriptRoot "..\.env")).Path

if (-not (Test-Path $envPath)) {
    Write-Error ".env not found at $envPath"
    return
}

$count = 0
Get-Content $envPath | ForEach-Object {
    $line = $_.Trim()
    if (-not $line) { return }
    if ($line.StartsWith("#")) { return }

    $idx = $line.IndexOf("=")
    if ($idx -lt 1) { return }

    $key = $line.Substring(0, $idx).Trim()
    $value = $line.Substring($idx + 1).Trim()

    # Strip surrounding quotes if present
    if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
        ($value.StartsWith("'") -and $value.EndsWith("'"))) {
        $value = $value.Substring(1, $value.Length - 2)
    }

    Set-Item -Path "env:$key" -Value $value
    $count++
}

Write-Host "Loaded $count env vars from $envPath"
