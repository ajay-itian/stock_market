# Implementation Tasks

## Task List

- [x] 1. Fix backend requirements.txt
  - Remove `uvicorn`, `sqlalchemy`, `apscheduler`, `pytz` from `backend/requirements.txt`
  - Add `mangum>=0.17.0` and `boto3>=1.34.0`
  - Result: `backend/requirements.txt` contains exactly the 6 Lambda-compatible packages
  - **Files**: `backend/requirements.txt`

- [x] 2. Restructure backend as app/ Python package
  - Create `backend/app/__init__.py` (empty)
  - Move `backend/main.py` → `backend/app/main.py`
  - Move `backend/refresh_handler.py` → `backend/app/refresh_handler.py`
  - Verify `refresh_handler.py` imports use `from app.main import ...`
  - **Files**: `backend/app/__init__.py`, `backend/app/main.py`, `backend/app/refresh_handler.py`

- [x] 3. Fix samconfig.toml paths
  - Change all `template = "sam/template.yaml"` → `template = "template.yaml"`
  - Change all `template_file = "sam/template.yaml"` → `template_file = "template.yaml"`
  - Add `ScreenerAdminKey=` to `[dev.deploy.parameters]` parameter_overrides (empty = no auth)
  - **Files**: `backend/samconfig.toml`

- [x] 4. Create backend build script
  - Create `backend/build.ps1` that cleans `dist/`, installs deps to `dist/layer/python/`, copies `app/` to `dist/function/app/`
  - **Files**: `backend/build.ps1`

- [x] 5. Create backend deploy script
  - Create `backend/deploy.ps1` that calls `build.ps1`, then runs `sam build` and `sam deploy --config-env dev --resolve-s3 --no-confirm-changeset`
  - **Files**: `backend/deploy.ps1`

- [x] 6. Fix frontend API base URL
  - Change `const BASE = "/api"` → `const BASE = import.meta.env.VITE_API_BASE_URL ?? "/api"` in `screener-ui/src/api.ts`
  - Update `screener-ui/.env.example` to document `VITE_API_BASE_URL`
  - **Files**: `screener-ui/src/api.ts`, `screener-ui/.env.example`

- [x] 7. Create frontend deploy script
  - Create `screener-ui/deploy-frontend.ps1` that accepts `$S3_BUCKET` and `$API_URL` params, sets `VITE_API_BASE_URL`, runs `npm run build`, syncs to S3
  - **Files**: `screener-ui/deploy-frontend.ps1`

- [x] 8. Create local env placeholder
  - Create `backend/sam/events/local-env.json` with DynamoDB table name overrides for local dev
  - **Files**: `backend/sam/events/local-env.json`
