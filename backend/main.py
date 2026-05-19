"""Local FastAPI entrypoint for development.

This file delegates to the same `backend/app/main.py` application
used by the AWS Lambda deployment package.
"""

import os
import sys

ROOT_DIR = os.path.dirname(__file__)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# Local development should create missing DynamoDB tables automatically.
os.environ.setdefault("DYNAMODB_AUTO_CREATE_TABLES", "true")

from mangum import Mangum

from app.main import app

handler = Mangum(app)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
