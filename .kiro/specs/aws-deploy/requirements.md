# Requirements Document

## Introduction

Deploy the Indian Equity Screener to AWS in the `ap-south-1` region for a `dev` environment. The system consists of a FastAPI backend (Lambda + API Gateway via SAM/CloudFormation) and a Vite/React frontend (S3 + CloudFront). Before deployment can succeed, several pre-existing code and configuration issues must be fixed: stale Python dependencies, missing Python package structure, incorrect SAM config paths, absent build scripts, and a hardcoded API base URL in the frontend.

## Glossary

- **Backend**: The FastAPI application in `backend/`, adapted for AWS Lambda via Mangum.
- **Frontend**: The Vite/React application in `screener-ui/`, served as static assets from S3 + CloudFront.
- **SAM**: AWS Serverless Application Model CLI and template format used to define and deploy Lambda, API Gateway, DynamoDB, and EventBridge resources.
- **Layer**: An AWS Lambda Layer (`DependenciesLayer`) that packages Python dependencies separately from the function code to keep deployment zips small.
- **Function package**: The `dist/function/` directory containing the `app/` Python package (handler code only, no deps).
- **Layer package**: The `dist/layer/python/` directory containing installed Python dependencies in the Lambda layer layout.
- **App package**: The `app/` Python package inside `backend/` containing `__init__.py`, `main.py`, and `refresh_handler.py`.
- **samconfig.toml**: SAM CLI configuration file that stores per-environment deploy parameters.
- **build.ps1**: PowerShell build script that produces `dist/layer/` and `dist/function/` artifacts.
- **deploy.ps1**: PowerShell deploy script that runs the build then invokes `sam deploy`.
- **deploy-frontend.ps1**: PowerShell script that builds the frontend and syncs assets to S3.
- **VITE_API_BASE_URL**: Vite environment variable that sets the API Gateway base URL for production builds.
- **CloudFront**: AWS CDN used to serve the frontend with HTTPS and caching.
- **EventBridge Scheduler**: AWS service that triggers the refresh Lambda on a schedule.

## Requirements

### Requirement 1: Fix Backend Python Dependencies

**User Story:** As a developer, I want the `requirements.txt` to contain only Lambda-compatible dependencies, so that the Lambda layer builds correctly without pulling in server-process or SQLite-era packages.

#### Acceptance Criteria

1. THE `backend/requirements.txt` SHALL list exactly these packages (with pinned minimum versions): `fastapi>=0.111.0`, `mangum>=0.17.0`, `boto3>=1.34.0`, `yfinance>=0.2.38`, `pydantic>=2.0.0`, `gnews>=0.3.7`.
2. THE `backend/requirements.txt` SHALL NOT contain `uvicorn`, `sqlalchemy`, `apscheduler`, or `pytz`.
3. WHEN the layer build script installs from `requirements.txt`, THE Build_Script SHALL produce a `dist/layer/python/` directory containing importable packages for all listed dependencies.

### Requirement 2: Restructure Backend as Python Package

**User Story:** As a developer, I want the backend source files organised as an `app/` Python package, so that Lambda handler references (`app.main.handler`, `app.refresh_handler.handler`) resolve correctly at runtime.

#### Acceptance Criteria

1. THE `backend/app/` directory SHALL exist and contain an `__init__.py` file (may be empty).
2. THE `backend/app/main.py` file SHALL contain the FastAPI application and the Mangum `handler` object.
3. THE `backend/app/refresh_handler.py` file SHALL contain the EventBridge Lambda handler function named `handler`.
4. WHEN `refresh_handler.py` imports from the main module, THE import statement SHALL use `from app.main import ...` so it resolves correctly inside the Lambda execution environment.
5. THE `backend/template.yaml` handler references SHALL remain `app.main.handler` and `app.refresh_handler.handler` without modification.

### Requirement 3: Fix SAM Configuration Paths

**User Story:** As a developer, I want `samconfig.toml` to reference the correct template and event file paths, so that `sam build` and `sam deploy` commands succeed when run from the `backend/` directory.

#### Acceptance Criteria

1. THE `backend/samconfig.toml` `[default.build.parameters]` section SHALL set `template = "template.yaml"` (relative to `backend/`).
2. THE `backend/samconfig.toml` `[default.local_invoke.parameters]` and `[default.local_start_api.parameters]` sections SHALL set `template = "template.yaml"`.
3. THE `backend/samconfig.toml` `[default.local_invoke.parameters]` and `[default.local_start_api.parameters]` sections SHALL set `env_vars = "sam/events/local-env.json"`.
4. THE `backend/samconfig.toml` `[dev.deploy.parameters]` section SHALL set `template_file = "template.yaml"`.
5. THE `backend/samconfig.toml` `[dev.deploy.parameters]` `parameter_overrides` SHALL include `"ScreenerAdminKey="` (empty string, no auth for dev).
6. THE `backend/samconfig.toml` `[dev.deploy.parameters]` SHALL set `region = "ap-south-1"`.

### Requirement 4: Create Backend Build Script

**User Story:** As a developer, I want a `build.ps1` script in `backend/` that produces the Lambda layer and function artifacts, so that `sam deploy` can package and upload them without manual steps.

#### Acceptance Criteria

1. THE `backend/build.ps1` script SHALL install packages from `requirements.txt` into `dist/layer/python/` using `pip install -r requirements.txt -t dist/layer/python/ --upgrade`.
2. THE `backend/build.ps1` script SHALL copy the `app/` directory into `dist/function/app/` so the function package contains only handler code.
3. WHEN `build.ps1` runs, THE Build_Script SHALL remove and recreate `dist/layer/` and `dist/function/` directories before populating them, ensuring a clean build.
4. WHEN `build.ps1` completes successfully, THE `dist/layer/python/` directory SHALL contain installed packages and THE `dist/function/app/` directory SHALL contain `__init__.py`, `main.py`, and `refresh_handler.py`.

### Requirement 5: Create Backend Deploy Script

**User Story:** As a developer, I want a single `deploy.ps1` script in `backend/` that builds artifacts and deploys the SAM stack, so that the entire backend deployment is one command.

#### Acceptance Criteria

1. THE `backend/deploy.ps1` script SHALL invoke `build.ps1` before running any SAM commands.
2. WHEN the build step succeeds, THE `backend/deploy.ps1` script SHALL run `sam deploy --config-env dev` from the `backend/` directory.
3. IF the SAM S3 artifact bucket does not yet exist, THEN THE `backend/deploy.ps1` script SHALL pass `--resolve-s3` to `sam deploy` so SAM auto-creates a managed bucket.
4. THE `backend/deploy.ps1` script SHALL pass `--no-confirm-changeset` for non-interactive execution.

### Requirement 6: Fix Frontend API Base URL

**User Story:** As a developer, I want the frontend to read the API Gateway URL from a Vite environment variable, so that production builds point to the deployed API Gateway instead of the relative `/api` path.

#### Acceptance Criteria

1. THE `screener-ui/src/api.ts` `BASE` constant SHALL be set to `import.meta.env.VITE_API_BASE_URL ?? "/api"`.
2. WHEN `VITE_API_BASE_URL` is not set, THE Frontend SHALL fall back to `"/api"` so local development with the Vite proxy continues to work unchanged.
3. THE `screener-ui/.env.example` file SHALL document the `VITE_API_BASE_URL` variable with an example value and explanation.

### Requirement 7: Create Frontend Deploy Script

**User Story:** As a developer, I want a `deploy-frontend.ps1` script in `screener-ui/` that builds and uploads the frontend to S3, so that the frontend can be deployed with a single command after the backend stack is up.

#### Acceptance Criteria

1. THE `screener-ui/deploy-frontend.ps1` script SHALL accept `$S3_BUCKET` and `$API_URL` as parameters (or read them from environment variables).
2. WHEN invoked, THE `screener-ui/deploy-frontend.ps1` script SHALL run `npm run build` with `VITE_API_BASE_URL` set to the provided API URL.
3. WHEN the build succeeds, THE `screener-ui/deploy-frontend.ps1` script SHALL sync the `dist/` output to the specified S3 bucket using `aws s3 sync dist/ s3://$S3_BUCKET --delete`.
4. THE `screener-ui/deploy-frontend.ps1` script SHALL print the S3 bucket URL and a reminder to invalidate the CloudFront distribution cache after upload.

### Requirement 8: Create Local Environment Placeholder

**User Story:** As a developer, I want a `backend/sam/events/local-env.json` placeholder file, so that `sam local start-api` does not fail due to a missing env-vars file referenced in `samconfig.toml`.

#### Acceptance Criteria

1. THE `backend/sam/events/local-env.json` file SHALL exist and contain a valid JSON object with at minimum the `Parameters` key mapping to an empty object or representative local overrides for DynamoDB table names.
2. WHEN `sam local start-api` is invoked, THE SAM_CLI SHALL load the env-vars file without error.
