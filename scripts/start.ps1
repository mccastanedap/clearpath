# Bootstrap a dev session: activate the venv and load .env, then sanity-check env vars.
# Dot-source so the activation and env vars persist:
#   . .\scripts\start.ps1

$ErrorActionPreference = "Stop"

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$venvActivate = Join-Path $projectRoot "venv\Scripts\Activate.ps1"

if (-not (Test-Path $venvActivate)) {
    Write-Error "venv not found at $venvActivate. Create it with: python -m venv venv"
    return
}

. $venvActivate
. (Join-Path $PSScriptRoot "load-env.ps1")

Write-Host ""
Write-Host "--- Environment ---"
Write-Host "Python:  $(python --version)"
Write-Host "Project: $projectRoot"
Write-Host ""

$required = @(
    "SUPABASE_HOST",
    "SUPABASE_PORT",
    "SUPABASE_USER",
    "SUPABASE_PASSWORD",
    "SUPABASE_DATABASE",
    "S3_BUCKET_NAME",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "ANTHROPIC_API_KEY"
)
$optional = @(
    "SENDGRID_API_KEY",
    "FROM_EMAIL",
    "REPORT_RECIPIENT_EMAIL"
)

Write-Host "--- Required env vars ---"
foreach ($name in $required) {
    $value = [Environment]::GetEnvironmentVariable($name, "Process")
    $status = if ($value) { "set" } else { "MISSING" }
    Write-Host ("  {0,-25} {1}" -f $name, $status)
}

Write-Host ""
Write-Host "--- Optional env vars ---"
foreach ($name in $optional) {
    $value = [Environment]::GetEnvironmentVariable($name, "Process")
    $status = if ($value) { "set" } else { "not set" }
    Write-Host ("  {0,-25} {1}" -f $name, $status)
}

Write-Host ""
Write-Host "Ready. Run: python main.py  |  python -m dbt.cli.main run"
