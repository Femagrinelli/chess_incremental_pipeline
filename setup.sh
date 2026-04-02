#!/usr/bin/env bash
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

info()    { echo -e "${CYAN}▸${NC} $*"; }
success() { echo -e "${GREEN}✓${NC} $*"; }
warn()    { echo -e "${YELLOW}⚠${NC}  $*"; }
error()   { echo -e "${RED}✗ ERROR:${NC} $*"; exit 1; }
header()  { echo -e "\n${BOLD}$*${NC}"; }

header "[ 1/5 ] Checking dependencies"
command -v docker >/dev/null 2>&1 || error "Docker not found."
command -v python3 >/dev/null 2>&1 || error "python3 not found."

if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
else
    error "Docker Compose not found."
fi

success "Docker, Python and Compose are available."

header "[ 2/5 ] Preparing environment"
if [ ! -f .env ]; then
    cp .env.example .env
    success "Copied .env.example to .env"
else
    warn ".env already exists, keeping it."
fi

if grep -q "^FERNET_KEY=$" .env; then
    FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" 2>/dev/null || python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
    sed -i "s|^FERNET_KEY=$|FERNET_KEY=${FERNET_KEY}|" .env
    success "Generated FERNET_KEY"
fi

if grep -q "^SECRET_KEY=$" .env; then
    SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s|^SECRET_KEY=$|SECRET_KEY=${SECRET_KEY}|" .env
    success "Generated SECRET_KEY"
fi

if grep -q "^AWS_ACCESS_KEY_ID=your_aws_access_key_id$" .env; then
    error "AWS_ACCESS_KEY_ID is still the example value."
fi

if grep -q "^AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key$" .env; then
    error "AWS_SECRET_ACCESS_KEY is still the example value."
fi

if grep -q "^S3_BUCKET=your_existing_s3_bucket$" .env; then
    error "S3_BUCKET is still the example value."
fi

if grep -q "^DBT_ATHENA_S3_STAGING_DIR=$" .env; then
    error "DBT_ATHENA_S3_STAGING_DIR must point to a writable S3 staging prefix."
fi

if grep -q "^DBT_ATHENA_S3_STAGING_DIR=s3://your_existing_s3_bucket/dbt-athena-staging/$" .env; then
    error "DBT_ATHENA_S3_STAGING_DIR is still the example value."
fi

header "[ 3/5 ] Creating local directories"
mkdir -p airflow/dags
mkdir -p airflow/logs
mkdir -p airflow/plugins
mkdir -p scripts
mkdir -p dbt/chess_medallion
mkdir -p dbt/profiles
chmod -R 777 airflow/logs
success "Directories are ready."

header "[ 4/5 ] Building images"
info "Building Docker images..."
$COMPOSE build
success "Images built."

header "[ 5/5 ] Starting services"
info "Starting stack..."
$COMPOSE up -d

info "Waiting for Airflow webserver..."
MAX_WAIT=180
ELAPSED=0
until $COMPOSE exec -T airflow-webserver curl -sf http://localhost:8080/health >/dev/null 2>&1; do
    if [ "$ELAPSED" -ge "$MAX_WAIT" ]; then
        warn "Airflow is still starting. Check logs with: $COMPOSE logs airflow-webserver"
        break
    fi
    echo -n "."
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done
echo ""

VPS_IP=$(hostname -I | awk '{print $1}' 2>/dev/null || echo "localhost")
AIRFLOW_USER=$(grep "^AIRFLOW_ADMIN_USER=" .env | cut -d= -f2)
AIRFLOW_PASS=$(grep "^AIRFLOW_ADMIN_PASSWORD=" .env | cut -d= -f2)
S3_BUCKET=$(grep "^S3_BUCKET=" .env | cut -d= -f2)

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}  Chess incremental pipeline is up${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${BOLD}Airflow UI${NC}  → http://${VPS_IP}:8080"
echo -e "              login: ${CYAN}${AIRFLOW_USER}${NC} / ${CYAN}${AIRFLOW_PASS}${NC}"
echo -e "  ${BOLD}S3 Bucket${NC}   → s3://${S3_BUCKET}"
echo ""
echo -e "  ${BOLD}Useful commands${NC}"
echo -e "    make logs"
echo -e "    make ps"
echo -e "    make trigger-bootstrap"
echo -e "    make trigger-core"
echo -e "    make trigger-backfill"
echo -e "    make trigger-bronze"
echo -e "    make trigger-bronze-backfill MONTH_KEY=2026-02"
echo -e "    make trigger-bronze-backfill BRONZE_CONF='{\"year\":\"2026\"}'"
echo -e "    make trigger-dbt-silver"
echo -e "    make trigger-dbt-silver-backfill MONTH_KEY=2026-02"
echo -e "    make trigger-dbt-silver-backfill DBT_SILVER_CONF='{\"year\":\"2026\"}'"
echo -e "    make trigger-dbt-gold"
echo -e "    make trigger-dbt-gold-backfill DBT_GOLD_CONF='{\"year\":\"2026\"}'"
echo -e "    make dbt-debug"
echo -e "    make dbt-ls"
echo -e "    make down"
