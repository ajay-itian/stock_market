# Design Document

## Overview

This design covers the concrete file changes and new artifacts needed to deploy the Indian Equity Screener to AWS (`ap-south-1`, `dev` environment) using AWS SAM and the AWS CLI. The work is purely mechanical: fix broken configs, restructure source files, write build/deploy scripts, and wire the frontend to the deployed API URL.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Developer machine (Windows, PowerShell)                │
│                                                         │
│  backend/deploy.ps1                                     │
│    └─ build.ps1  →  dist/layer/python/  (pip deps)     │
│                  →  dist/function/app/  (handler code)  │
│    └─ sam build  →  .aws-sam/build/                     │
│    └─ sam deploy →  CloudFormation stack                │
│                                                         │
│  screener-ui/deploy-frontend.ps1                        │
│    └─ npm run build  →  dist/                           │
│    └─ aws s3 sync    →  S3 bucket                       │
└─────────────────────────────────────────────────────────┘

AWS ap-south-1
┌──────────────────────────────────────────────────────────┐
│  CloudFormation stack: equity-screener-dev               │
│                                                          │
│  API Gateway (HTTP API v2)                               │
│    └─ ANY /{proxy+}  →  ApiFunction (Lambda)             │
│         handler: app.main.handler                        │
│         layer:   DependenciesLayer                       │
│                                                          │
│  RefreshFunction (Lambda, 15-min timeout)                │
│    handler: app.refresh_handler.handler                  │
│    triggered by: EventBridge Scheduler (daily + weekly)  │
│                                                          │
│  DynamoDB tables (PAY_PER_REQUEST):                      │
│    equity-screener-dev-quotes                            │
│    equity-screener-dev-info                              │
│    equity-screener-dev-financials                        │
│    equity-screener-dev-balance-sheet                     │
│    equity-screener-dev-history                           │
│    equity-screener-dev-meta                              │
│    equity-screener-dev-news-cache (TTL enabled)          │
│                                                          │
│  S3 bucket (frontend static assets)                      │
│  CloudFront distribution → S3 bucket                    │
└──────────────────────────────────────────────────────────┘
```

## Components

### 1. Backend Python Package (`backend/app/`)

The Lambda handler references `app.main.handler` and `app.refresh_handler.handler`. This requires the source files to live inside an `app/` package directory.

**File layout after restructure:**
```
backend/
  app/
    __init__.py          (empty)
    main.py              (moved from backend/main.py)
    refresh_handler.py   (moved from backend/refresh_handler.py)
  requirements.txt       (fixed)
  template.yaml          (unchanged)
  samconfig.toml         (paths fixed)
  build.ps1              (new)
  deploy.ps1             (new)
  sam/
    events/
      local-env.json     (new placeholder)
```

### 2. Fixed `requirements.txt`

Remove server-process and SQLite-era packages; add `mangum` and pin `boto3`:

```
fastapi>=0.111.0
mangum>=0.17.0
boto3>=1.34.0
yfinance>=0.2.38
pydantic>=2.0.0
gnews>=0.3.7
```

### 3. Fixed `samconfig.toml`

All `template` / `template_file` references change from `sam/template.yaml` → `template.yaml` (SAM is run from `backend/`). The `dev` profile gets `ScreenerAdminKey=` (empty = no auth).

### 4. Build Script (`backend/build.ps1`)

```
Clean dist/
pip install -r requirements.txt -t dist/layer/python/ --upgrade
Copy-Item app/ → dist/function/app/ (recursive)
```

The layer layout `dist/layer/python/` is what Lambda expects for a Python layer.

### 5. Deploy Script (`backend/deploy.ps1`)

```
.\build.ps1
sam build
sam deploy --config-env dev --resolve-s3 --no-confirm-changeset
```

`--resolve-s3` lets SAM auto-create a managed S3 bucket for artifacts on first run, avoiding the need to pre-create one.

### 6. Frontend API URL (`screener-ui/src/api.ts`)

Change:
```ts
const BASE = "/api";
```
To:
```ts
const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api";
```

This is a one-line change. Local dev (Vite proxy) continues to work when `VITE_API_BASE_URL` is unset.

### 7. Frontend Deploy Script (`screener-ui/deploy-frontend.ps1`)

```powershell
param($S3_BUCKET, $API_URL)
$env:VITE_API_BASE_URL = $API_URL
npm run build
aws s3 sync dist/ s3://$S3_BUCKET --delete
```

### 8. Local Env Placeholder (`backend/sam/events/local-env.json`)

Minimal JSON so `sam local start-api` doesn't fail:
```json
{
  "Parameters": {
    "DDB_TBL_QUOTES": "equity-screener-dev-quotes",
    ...
  }
}
```

## Data Flow

1. Developer runs `backend/deploy.ps1`
2. `build.ps1` installs deps into `dist/layer/python/` and copies `app/` into `dist/function/app/`
3. `sam build` packages the layer and function using the artifacts in `dist/`
4. `sam deploy --config-env dev` creates/updates the CloudFormation stack
5. Stack outputs the API Gateway URL
6. Developer runs `screener-ui/deploy-frontend.ps1 -S3_BUCKET <bucket> -API_URL <url>`
7. Frontend builds with `VITE_API_BASE_URL` set, output synced to S3

## Error Handling

- `build.ps1` uses `$ErrorActionPreference = "Stop"` so any pip or copy failure aborts immediately
- `deploy.ps1` checks the exit code of `build.ps1` before proceeding to SAM commands
- `deploy-frontend.ps1` checks npm build exit code before syncing to S3
