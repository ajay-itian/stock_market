$ErrorActionPreference = "Stop"

Write-Host "=== Deploying Backend to AWS (dev) ===" -ForegroundColor Cyan

# Step 1: Build Lambda artifacts
Write-Host ""
Write-Host "--- Step 1: Building artifacts ---" -ForegroundColor Yellow
.\build.ps1

# Step 2: SAM build
Write-Host ""
Write-Host "--- Step 2: sam build ---" -ForegroundColor Yellow
sam build

# Step 3: SAM deploy
# --resolve-s3            : auto-creates a SAM-managed S3 bucket for artifacts on first run
# --no-confirm-changeset  : non-interactive execution (no prompt before applying changes)
Write-Host ""
Write-Host "--- Step 3: sam deploy ---" -ForegroundColor Yellow
sam deploy --config-env dev --resolve-s3 --no-confirm-changeset

Write-Host ""
Write-Host "=== Deploy complete ===" -ForegroundColor Green
Write-Host "Note the API Gateway URL from the stack outputs above and use it when deploying the frontend."
