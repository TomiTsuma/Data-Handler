# skill: ubuntu-sysadmin
# Trigger: "Ubuntu", "Linux", "bash", "terminal", "cron", "systemd", "service",
#          "port", "process", "kill", "venv", "virtual environment", "pip install",
#          "permission", "chmod", "disk space", "memory", "GPU", "CUDA", "nvidia",
#          "SSH", "firewall", "ufw", "apt", "package", "VS Code server"

## Purpose
Ubuntu 24 system administration for Tomi's development machine and production servers.
Covers: Python environment management, GPU setup, process management, cron,
network/firewall, SSH, disk management, and common troubleshooting patterns.

---

## Python Environment Management

```bash
# ── Virtual environments ─────────────────────────────────────────────────────

# Create project venv (always use venv, not conda, for production parity)
python3.11 -m venv .venv
source .venv/bin/activate

# Verify which Python is active
which python && python --version

# Install from requirements with hash verification (production)
pip install -r requirements.txt --require-hashes

# Freeze current environment
pip freeze > requirements.txt

# Export only top-level packages (cleaner)
pip install pip-tools
pip-compile pyproject.toml -o requirements.txt

# Upgrade all packages
pip install --upgrade $(pip freeze | cut -d= -f1)

# ── Common pip issues on Ubuntu 24 ──────────────────────────────────────────

# "externally-managed-environment" error (Ubuntu 24 default)
pip install <package> --break-system-packages   # quick fix
# Better: use venv (above) or:
pipx install <cli-tool>   # for CLI tools

# Install specific Python version (Ubuntu 24)
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.11 python3.11-venv python3.11-dev

# Switch default python
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1
sudo update-alternatives --config python3
```

---

## GPU / CUDA Management

```bash
# ── CUDA and GPU monitoring ──────────────────────────────────────────────────

# Check GPU status
nvidia-smi
nvidia-smi -l 1              # refresh every 1 second
nvidia-smi --query-gpu=memory.used,memory.free,utilization.gpu --format=csv

# GPU memory usage by process
nvidia-smi pmon -s u         # per-process GPU utilization
fuser /dev/nvidia*           # PIDs using GPU

# Check CUDA version
nvcc --version
python -c "import torch; print(torch.cuda.is_available(), torch.version.cuda)"

# Install PyTorch with specific CUDA version (Ubuntu 24)
# Always get the exact command from https://pytorch.org/get-started/locally/
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu121

# Clear GPU memory (kill all processes using GPU)
sudo fuser -v /dev/nvidia* | awk '{print $2}' | xargs sudo kill -9

# Monitor training in real-time
watch -n 2 nvidia-smi

# ── PyTorch GPU debugging ────────────────────────────────────────────────────

# Check if tensor is on GPU
python -c "
import torch
x = torch.randn(3, 3)
print('Device:', x.device)
x = x.cuda()
print('After .cuda():', x.device)
print('Allocated:', torch.cuda.memory_allocated() / 1e9, 'GB')
"

# Find CUDA memory leak
python -c "
import torch
import gc
gc.collect()
torch.cuda.empty_cache()
print('After cleanup:', torch.cuda.memory_allocated())
"
```

---

## Process Management

```bash
# ── Finding and killing processes ────────────────────────────────────────────

# Find process using a port
sudo ss -tlnp | grep :8000
sudo lsof -i :8000
sudo fuser -k 8000/tcp           # kill whatever is on port 8000

# Find Python processes
ps aux | grep python
pgrep -la python

# Kill by port (common for FastAPI/React dev server)
kill $(lsof -t -i:8000)
kill $(lsof -t -i:3000)         # React dev server

# Run with non-default port
PORT=3001 npm start              # React
uvicorn api.main:app --port 8001

# ── Background processes ──────────────────────────────────────────────────────

# Run training in background, log output
nohup python -m ml.pipelines.train > logs/train.log 2>&1 &
echo "PID: $!"

# Screen session for long training runs
screen -S training
python -m ml.pipelines.train
# Ctrl+A, D to detach — session keeps running
screen -r training              # reattach

# Tmux (preferred over screen)
tmux new -s training
# Ctrl+B, D to detach
tmux attach -t training

# ── systemd services (for production daemons) ─────────────────────────────────

# Create service file
sudo cat > /etc/systemd/system/core-outline-api.service << 'EOF'
[Unit]
Description=Core&Outline FastAPI
After=network.target

[Service]
Type=exec
User=tomi
WorkingDirectory=/opt/core-outline
Environment=PATH=/opt/core-outline/.venv/bin
ExecStart=/opt/core-outline/.venv/bin/uvicorn api.main:app --host 0.0.0.0 --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable core-outline-api
sudo systemctl start core-outline-api
sudo systemctl status core-outline-api
journalctl -u core-outline-api -f   # follow logs
```

---

## Cron Jobs

```bash
# Edit crontab
crontab -e

# ── Tomi's cron schedule ──────────────────────────────────────────────────────
# All times in EAT (UTC+3) — convert to UTC for cron (subtract 3)

# Daily metrics Slack post at 7 AM EAT (4 AM UTC)
0 4 * * * cd /home/tomi/core-outline && .venv/bin/python scripts/slack_post.py --template daily_metrics >> logs/cron.log 2>&1

# Nightly data ingestion at 1 AM EAT (10 PM UTC)
0 22 * * * cd /home/tomi/core-outline && .venv/bin/python -m data.pipelines.daily_ingestion >> logs/ingestion.log 2>&1

# Weekly churn scoring every Monday at 2 AM EAT (11 PM UTC Sunday)
0 23 * * 0 cd /home/tomi/core-outline && .venv/bin/python -m ml.churn.early_warning >> logs/churn.log 2>&1

# Monthly financial summary on 1st at 8 AM EAT (5 AM UTC)
0 5 1 * * cd /home/tomi && .venv/bin/python scripts/financial_summary.py >> logs/finance.log 2>&1

# Log rotation — keep last 7 days of logs
0 3 * * * find /home/tomi/core-outline/logs -name "*.log" -mtime +7 -delete

# ── Cron debugging ──────────────────────────────────────────────────────────
# Cron runs in minimal environment — always use absolute paths
# Test cron command manually first:
/bin/bash -c 'cd /home/tomi/core-outline && .venv/bin/python scripts/slack_post.py'

# Check if cron ran
grep CRON /var/log/syslog | tail -20

# Common cron issue: PATH not set — add to top of crontab
PATH=/home/tomi/core-outline/.venv/bin:/usr/bin:/bin
```

---

## Network and Firewall

```bash
# ── UFW firewall ──────────────────────────────────────────────────────────────

# Status
sudo ufw status verbose

# Allow common ports
sudo ufw allow ssh
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw allow 8000/tcp           # FastAPI dev
sudo ufw allow from 10.0.0.0/8   # allow internal network

# Enable
sudo ufw enable

# ── Port forwarding for local dev ────────────────────────────────────────────

# Forward remote port 8000 to local machine (access prod API locally)
ssh -L 5432:localhost:5432 ubuntu@prod-server    # PostgreSQL
ssh -L 8000:localhost:8000 ubuntu@staging-server  # API

# ── Network debugging ────────────────────────────────────────────────────────

# Check what's listening
sudo ss -tlnp
sudo netstat -tlnp

# Test endpoint
curl -I https://core-outline.com/health
curl -X POST http://localhost:8000/api/v1/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [1, 2, 3]}'

# DNS debugging
nslookup core-outline.com
dig core-outline.com
```

---

## Disk and Memory Management

```bash
# ── Disk usage ────────────────────────────────────────────────────────────────

# Overall disk usage
df -h

# Find large files/directories
du -sh /* 2>/dev/null | sort -rh | head -20
du -sh ~/core-outline/* | sort -rh | head -10

# Find and clean Docker artifacts (major disk consumer)
docker system df
docker system prune -a --volumes   # WARNING: removes all stopped containers + unused images

# Clean old pip cache
pip cache purge

# Clean Python __pycache__
find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null
find . -name "*.pyc" -delete

# ── Memory ────────────────────────────────────────────────────────────────────

# Memory usage overview
free -h
vmstat -s | head -10

# Top memory consumers
ps aux --sort=-%mem | head -10

# Find OOM-killed processes
dmesg | grep -i "killed process"

# ── Swap management ──────────────────────────────────────────────────────────

# Check swap
swapon --show

# Add swap file (for memory-constrained training)
sudo fallocate -l 8G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# Make permanent:
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

---

## VS Code and Development Setup

```bash
# ── VS Code Remote Development ────────────────────────────────────────────────

# Install VS Code Server on remote machine
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > microsoft.gpg
sudo install -o root -g root -m 644 microsoft.gpg /etc/apt/trusted.gpg.d/
sudo sh -c 'echo "deb [arch=amd64,arm64,armhf] https://packages.microsoft.com/repos/code stable main" > /etc/apt/sources.list.d/vscode.list'
sudo apt update && sudo apt install code

# Connect to remote via SSH tunnel in VS Code:
# Extensions > Remote-SSH > Connect to Host > tomi@server-ip

# ── Jupyter on remote server ─────────────────────────────────────────────────

# Start Jupyter on server (no browser)
jupyter lab --no-browser --port 8888 --ip=0.0.0.0

# Access locally via SSH tunnel
ssh -L 8888:localhost:8888 ubuntu@server-ip
# Then open http://localhost:8888 in local browser

# ── Environment variables ─────────────────────────────────────────────────────

# .env file (never commit to git)
cat > .env << 'EOF'
ANTHROPIC_API_KEY=sk-ant-...
DATABASE_URL=postgresql://postgres:password@localhost:5432/core_outline
REDIS_URL=redis://localhost:6379
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
SLACK_WEBHOOK_URL=https://hooks.slack.com/...
EOF

# Load .env in shell
set -a && source .env && set +a

# Load in Python
from dotenv import load_dotenv
load_dotenv()   # reads .env automatically

# ── Git config ────────────────────────────────────────────────────────────────

git config --global user.name "Tomi"
git config --global user.email "tomi@core-outline.com"
git config --global core.editor "code --wait"
git config --global pull.rebase true

# Conventional commit helper
alias gc='git commit -m'
# Usage: gc "feat(churn): add SHAP explainability to model"
# Format: type(scope): description
# Types: feat, fix, docs, refactor, test, chore, perf
```

---

## Common Troubleshooting Reference

| Problem | Diagnosis | Fix |
|---|---|---|
| `pip: command not found` | Python PATH issue | `python3 -m pip install ...` |
| `Port 8000 already in use` | Previous process still running | `kill $(lsof -t -i:8000)` |
| `CUDA out of memory` | GPU memory leak | `torch.cuda.empty_cache()` + reduce batch size |
| `Permission denied` on script | Missing execute bit | `chmod +x script.sh` |
| Cron job not running | Path issue in cron | Use absolute paths, check syslog |
| SSH connection timeout | Firewall blocking | `sudo ufw allow ssh` |
| Disk full | Docker/logs/cache | `docker system prune -a` |
| `ModuleNotFoundError` | Wrong venv active | `which python` → activate correct venv |
| `SettingWithCopyWarning` | Pandas slice mutation | Use `.loc[]` or `.copy()` |
| Jupyter kernel dies | OOM | Add swap, reduce data size |
| `Connection refused` | Service not running | `systemctl status service-name` |
| `SSL: CERTIFICATE_VERIFY_FAILED` | Cert issue | `pip install certifi` |
