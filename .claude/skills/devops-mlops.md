# skill: devops-mlops
# Trigger: "Docker", "Jenkins", "CI/CD", "deploy", "pipeline", "AWS", "S3", "EC2",
#          "SageMaker", "Dockerfile", "docker-compose", "container", "model deployment",
#          "model serving", "model versioning", "MLflow", "monitoring", "health check",
#          "nginx", "reverse proxy", "environment variables", "secrets"

## Purpose
Full DevOps and MLOps stack for Core&Outline: Docker containerization, Jenkins
CI/CD pipelines, AWS infrastructure, model versioning and deployment, and
production monitoring.

## Stack
- Docker + Docker Compose
- Jenkins (CI/CD)
- AWS: S3 (data/models), EC2 (compute), SageMaker (managed training/inference)
- MLflow (experiment tracking + model registry)
- nginx (reverse proxy)
- GitHub (source control, webhooks to Jenkins)

---

## Dockerfile Templates

```dockerfile
# Dockerfile.api — Core&Outline FastAPI backend
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc g++ git curl \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Non-root user for security
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "4", "--loop", "uvloop"]
```

```dockerfile
# Dockerfile.ml — ML training container
FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

WORKDIR /workspace

COPY requirements.ml.txt .
RUN pip install --no-cache-dir -r requirements.ml.txt

COPY ml/ ./ml/
COPY research/ ./research/
COPY configs/ ./configs/

# For SageMaker: training script must be at /opt/ml/code/train.py
COPY ml/pipelines/sagemaker_train.py /opt/ml/code/train.py

ENV PYTHONPATH=/workspace
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "ml.pipelines.train"]
```

```dockerfile
# Dockerfile.ml-serve — Model inference server
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

COPY requirements.serve.txt .
RUN pip install --no-cache-dir -r requirements.serve.txt

COPY ml/serving/ ./ml/serving/
COPY api/routers/predict.py ./api/routers/

EXPOSE 8001

CMD ["uvicorn", "ml.serving.server:app", "--host", "0.0.0.0", "--port", "8001"]
```

---

## Docker Compose (Full Stack)

```yaml
# docker-compose.yml

version: "3.9"

services:
  api:
    build:
      context: .
      dockerfile: Dockerfile.api
    ports:
      - "8000:8000"
    environment:
      - DATABASE_URL=postgresql://postgres:${DB_PASSWORD}@db:5432/core_outline
      - REDIS_URL=redis://redis:6379
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - ENVIRONMENT=production
    depends_on:
      db:
        condition: service_healthy
      redis:
        condition: service_started
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  ml-serve:
    build:
      context: .
      dockerfile: Dockerfile.ml-serve
    ports:
      - "8001:8001"
    environment:
      - MODEL_PATH=/models
      - REDIS_URL=redis://redis:6379
    volumes:
      - ./models:/models:ro       # read-only model artifacts
    restart: unless-stopped

  db:
    image: postgres:16-alpine
    environment:
      - POSTGRES_DB=core_outline
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    command: redis-server --requirepass ${REDIS_PASSWORD}
    volumes:
      - redis_data:/data

  nginx:
    image: nginx:alpine
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
      - ./nginx/ssl:/etc/nginx/ssl:ro
    depends_on:
      - api

volumes:
  postgres_data:
  redis_data:
```

---

## Jenkins CI/CD Pipeline

```groovy
// Jenkinsfile — Core&Outline CI/CD pipeline

pipeline {
    agent any

    environment {
        AWS_REGION = "af-south-1"           // AWS Africa (Cape Town) — lowest latency for Kenya
        ECR_REGISTRY = "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
        IMAGE_API = "${ECR_REGISTRY}/core-outline-api"
        IMAGE_ML = "${ECR_REGISTRY}/core-outline-ml-serve"
        SLACK_CHANNEL = "#deployments"
    }

    stages {
        stage("Checkout") {
            steps {
                checkout scm
                script {
                    env.GIT_COMMIT_SHORT = sh(
                        script: "git rev-parse --short HEAD", returnStdout: true
                    ).trim()
                }
            }
        }

        stage("Test") {
            parallel {
                stage("Unit Tests") {
                    steps {
                        sh """
                            python -m pytest tests/unit/ \
                                -x -q \
                                --cov=ml --cov=api \
                                --cov-report=xml:coverage.xml
                        """
                    }
                    post {
                        always {
                            junit "test-results/*.xml"
                            cobertura coberturaReportFile: "coverage.xml"
                        }
                    }
                }
                stage("Lint + Type Check") {
                    steps {
                        sh "ruff check ml/ api/ research/"
                        sh "mypy ml/ api/ --ignore-missing-imports"
                    }
                }
                stage("Integration Tests") {
                    steps {
                        sh """
                            docker-compose -f docker-compose.test.yml up -d
                            sleep 10
                            python -m pytest tests/integration/ -q
                            docker-compose -f docker-compose.test.yml down
                        """
                    }
                }
            }
        }

        stage("Build Docker Images") {
            when {
                branch "main"
            }
            parallel {
                stage("Build API") {
                    steps {
                        sh """
                            docker build -t ${IMAGE_API}:${GIT_COMMIT_SHORT} \
                                -t ${IMAGE_API}:latest \
                                -f Dockerfile.api .
                        """
                    }
                }
                stage("Build ML Serve") {
                    steps {
                        sh """
                            docker build -t ${IMAGE_ML}:${GIT_COMMIT_SHORT} \
                                -t ${IMAGE_ML}:latest \
                                -f Dockerfile.ml-serve .
                        """
                    }
                }
            }
        }

        stage("Push to ECR") {
            when {
                branch "main"
            }
            steps {
                sh """
                    aws ecr get-login-password --region ${AWS_REGION} | \
                        docker login --username AWS --password-stdin ${ECR_REGISTRY}
                    docker push ${IMAGE_API}:${GIT_COMMIT_SHORT}
                    docker push ${IMAGE_API}:latest
                    docker push ${IMAGE_ML}:${GIT_COMMIT_SHORT}
                    docker push ${IMAGE_ML}:latest
                """
            }
        }

        stage("Deploy to Staging") {
            when {
                branch "main"
            }
            steps {
                sh """
                    ssh -o StrictHostKeyChecking=no ubuntu@${STAGING_HOST} \
                        "cd /opt/core-outline && \
                        IMAGE_TAG=${GIT_COMMIT_SHORT} docker-compose pull && \
                        IMAGE_TAG=${GIT_COMMIT_SHORT} docker-compose up -d --no-deps api ml-serve"
                """
                sh "python scripts/smoke_test.py --host ${STAGING_HOST}"
            }
        }

        stage("Deploy to Production") {
            when {
                branch "main"
            }
            input {
                message "Deploy to production?"
                ok "Deploy"
                submitter "tomi"
            }
            steps {
                sh """
                    ssh ubuntu@${PROD_HOST} \
                        "cd /opt/core-outline && \
                        IMAGE_TAG=${GIT_COMMIT_SHORT} docker-compose pull && \
                        IMAGE_TAG=${GIT_COMMIT_SHORT} docker-compose up -d --no-deps api ml-serve"
                """
            }
        }
    }

    post {
        success {
            slackSend(
                channel: env.SLACK_CHANNEL,
                color: "good",
                message: "✅ Deploy ${GIT_COMMIT_SHORT} succeeded — ${env.JOB_NAME}"
            )
        }
        failure {
            slackSend(
                channel: env.SLACK_CHANNEL,
                color: "danger",
                message: "❌ Build/deploy failed — ${env.JOB_NAME} | ${env.BUILD_URL}"
            )
        }
    }
}
```

---

## AWS Infrastructure

```python
# scripts/aws_setup.py
"""
AWS infrastructure management for Core&Outline.
Covers S3 (data lake), EC2 (compute), SageMaker (ML training/inference).
"""

import boto3
import json
from pathlib import Path

AWS_REGION = "af-south-1"
S3_BUCKET = "core-outline-data"


class S3Manager:
    """S3 operations for data lake and model artifact storage."""

    def __init__(self, region: str = AWS_REGION):
        self.s3 = boto3.client("s3", region_name=region)
        self.bucket = S3_BUCKET

    def upload_model(self, local_path: str, model_name: str, version: str) -> str:
        key = f"models/{model_name}/{version}/{Path(local_path).name}"
        self.s3.upload_file(local_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def download_model(self, model_name: str, version: str, local_dir: str) -> str:
        prefix = f"models/{model_name}/{version}/"
        objects = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        for obj in objects.get("Contents", []):
            local_path = Path(local_dir) / Path(obj["Key"]).name
            self.s3.download_file(self.bucket, obj["Key"], str(local_path))
        return local_dir

    def upload_dataset(self, local_path: str, dataset_name: str, partition: str = None) -> str:
        """Upload dataset with optional partitioning (e.g. partition='year=2025/month=03')."""
        key = f"data/{dataset_name}/" + (f"{partition}/" if partition else "") + Path(local_path).name
        self.s3.upload_file(local_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def list_model_versions(self, model_name: str) -> list[str]:
        prefix = f"models/{model_name}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        versions = set()
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, Delimiter="/"):
            for prefix_obj in page.get("CommonPrefixes", []):
                version = prefix_obj["Prefix"].split("/")[-2]
                versions.add(version)
        return sorted(versions)


class SageMakerManager:
    """SageMaker training jobs and endpoint management."""

    def __init__(self, region: str = AWS_REGION):
        self.sm = boto3.client("sagemaker", region_name=region)
        self.role_arn = f"arn:aws:iam::{boto3.client('sts').get_caller_identity()['Account']}:role/SageMakerRole"

    def launch_training_job(
        self,
        job_name: str,
        image_uri: str,
        instance_type: str = "ml.p3.2xlarge",
        input_s3_path: str = None,
        output_s3_path: str = None,
        hyperparams: dict = None,
    ) -> str:
        """Launch a SageMaker training job."""
        config = {
            "TrainingJobName": job_name,
            "AlgorithmSpecification": {
                "TrainingImage": image_uri,
                "TrainingInputMode": "File",
            },
            "RoleArn": self.role_arn,
            "OutputDataConfig": {"S3OutputPath": output_s3_path or f"s3://{S3_BUCKET}/training-output/"},
            "ResourceConfig": {
                "InstanceType": instance_type,
                "InstanceCount": 1,
                "VolumeSizeInGB": 50,
            },
            "StoppingCondition": {"MaxRuntimeInSeconds": 86400},  # 24h max
        }

        if input_s3_path:
            config["InputDataConfig"] = [{
                "ChannelName": "training",
                "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": input_s3_path}},
            }]

        if hyperparams:
            config["HyperParameters"] = {k: str(v) for k, v in hyperparams.items()}

        self.sm.create_training_job(**config)
        return job_name

    def deploy_endpoint(
        self,
        model_s3_uri: str,
        endpoint_name: str,
        image_uri: str,
        instance_type: str = "ml.m5.xlarge",
    ) -> str:
        """Deploy a model as a SageMaker endpoint."""
        model_name = f"{endpoint_name}-model"
        self.sm.create_model(
            ModelName=model_name,
            PrimaryContainer={"Image": image_uri, "ModelDataUrl": model_s3_uri},
            ExecutionRoleArn=self.role_arn,
        )
        config_name = f"{endpoint_name}-config"
        self.sm.create_endpoint_config(
            EndpointConfigName=config_name,
            ProductionVariants=[{
                "VariantName": "primary",
                "ModelName": model_name,
                "InstanceType": instance_type,
                "InitialInstanceCount": 1,
            }],
        )
        self.sm.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=config_name)
        return endpoint_name
```

---

## MLflow Model Registry

```python
# ml/mlops/model_registry.py
"""
MLflow experiment tracking and model registry.
Tracks all training runs, registers the best model, and manages staging/production.
"""

import mlflow
import mlflow.pytorch
import mlflow.sklearn
from pathlib import Path
from dataclasses import asdict


MLFLOW_TRACKING_URI = "http://localhost:5000"   # or S3 backend for production


def log_experiment(
    experiment_name: str,
    config: object,           # dataclass config
    metrics: dict,
    artifacts: dict = None,   # {artifact_name: local_path}
    tags: dict = None,
) -> str:
    """Log a training run to MLflow. Returns run_id."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run() as run:
        mlflow.log_params(asdict(config))
        mlflow.log_metrics(metrics)
        if tags:
            mlflow.set_tags(tags)
        if artifacts:
            for name, path in artifacts.items():
                mlflow.log_artifact(path, artifact_path=name)
        return run.info.run_id


def register_model(run_id: str, model_name: str, artifact_path: str = "model") -> str:
    """Register model from a run to the MLflow model registry."""
    model_uri = f"runs:/{run_id}/{artifact_path}"
    result = mlflow.register_model(model_uri, model_name)
    return result.version


def promote_to_production(model_name: str, version: str) -> None:
    """Move model version to production stage."""
    client = mlflow.tracking.MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)
    # Archive current production
    for mv in client.get_latest_versions(model_name, stages=["Production"]):
        client.transition_model_version_stage(model_name, mv.version, "Archived")
    # Promote new version
    client.transition_model_version_stage(model_name, version, "Production")


def load_production_model(model_name: str):
    """Load the current production model."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    return mlflow.pyfunc.load_model(f"models:/{model_name}/Production")
```

---

## Nginx Config

```nginx
# nginx/nginx.conf — Core&Outline reverse proxy

upstream api_backend {
    server api:8000;
    keepalive 32;
}

upstream ml_backend {
    server ml-serve:8001;
    keepalive 16;
}

server {
    listen 80;
    server_name core-outline.com www.core-outline.com;
    return 301 https://$host$request_uri;
}

server {
    listen 443 ssl http2;
    server_name core-outline.com;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;

    # API routes
    location /api/ {
        proxy_pass http://api_backend;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_http_version 1.1;
        proxy_set_header Connection "";
    }

    # SSE streaming for AI analyst
    location /api/ai/analyst/ {
        proxy_pass http://api_backend;
        proxy_buffering off;
        proxy_cache off;
        proxy_set_header Connection "";
        proxy_read_timeout 300s;
    }

    # ML inference
    location /api/predict/ {
        proxy_pass http://ml_backend;
        proxy_read_timeout 60s;
    }
}
```

---

## Smoke Test Script

```python
# scripts/smoke_test.py
"""Run after every deployment to verify key endpoints are alive."""

import requests
import sys


def smoke_test(host: str) -> bool:
    base = f"https://{host}"
    tests = [
        ("GET", f"{base}/health", 200),
        ("GET", f"{base}/api/v1/metrics/status", 200),
        ("POST", f"{base}/api/v1/auth/ping", 401),   # expect 401 without token
    ]
    all_pass = True
    for method, url, expected_status in tests:
        resp = requests.request(method, url, timeout=10, verify=False)
        status = "✅" if resp.status_code == expected_status else "❌"
        print(f"{status} {method} {url} → {resp.status_code} (expected {expected_status})")
        if resp.status_code != expected_status:
            all_pass = False
    return all_pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    args = parser.parse_args()
    success = smoke_test(args.host)
    sys.exit(0 if success else 1)
```

---

## Usage in Claude Code

```bash
# Build and start full stack locally
docker-compose up -d --build

# Run Jenkins pipeline manually (via Jenkins CLI)
java -jar jenkins-cli.jar -s http://localhost:8080 build core-outline-pipeline

# Upload trained model to S3
python scripts/aws_setup.py upload-model \
  --local models/dqfd_pricing.pt --name dqfd_pricing --version v1.2.0

# Launch SageMaker training job
python scripts/aws_setup.py launch-training \
  --job dqfd-pricing-v2 --instance ml.p3.2xlarge \
  --input s3://core-outline-data/data/pricing_history/

# Deploy model endpoint on SageMaker
python scripts/aws_setup.py deploy-endpoint \
  --model s3://core-outline-data/models/churn/v3/model.tar.gz \
  --endpoint churn-predictor-prod

# Run smoke tests after deploy
python scripts/smoke_test.py --host staging.core-outline.com
```
