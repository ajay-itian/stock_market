$ErrorActionPreference = "Stop"

Write-Host "=== Building Lambda artifacts ===" -ForegroundColor Cyan

# Clean and recreate dist/function/
Write-Host "Cleaning dist\function\ ..."
if (Test-Path "dist\function") { Remove-Item -Recurse -Force "dist\function" }
New-Item -ItemType Directory -Path "dist\function" | Out-Null

# Install Python dependencies directly into the function package
# (no separate layer — avoids CloudFormation early-validation ARN checks)
Write-Host "Installing dependencies into dist\function\ ..."
pip install -r requirements.txt -t dist\function\ --upgrade --quiet

# Copy handler code into the function package
Write-Host "Copying app\ into dist\function\app\ ..."
Copy-Item -Recurse "app" "dist\function\app"

Write-Host ""
Write-Host "=== Build complete ===" -ForegroundColor Green
Write-Host "  Function package : dist\function\"
Write-Host "  Handler code     : dist\function\app\"
