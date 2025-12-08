#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ANSIBLE_DIR="$SCRIPT_DIR/../ansible"

# Load .env if exists
if [ -f "$ANSIBLE_DIR/.env" ]; then
    echo "Loading secrets from .env..."
    export $(grep -v '^#' "$ANSIBLE_DIR/.env" | xargs)
else
    echo "Warning: .env file not found at $ANSIBLE_DIR/.env"
    echo "Copy .env.example to .env and fill in the secrets"
fi

usage() {
    echo "Usage: $0 <command> <environment> [options]"
    echo ""
    echo "Commands:"
    echo "  deploy     Deploy to target environment"
    echo "  rollback   Rollback deployment"
    echo "  status     Check deployment status"
    echo ""
    echo "Environments:"
    echo "  dev        Development environment"
    echo "  test       Test environment"
    echo "  prod       Production environment"
    echo ""
    echo "Options:"
    echo "  --component <name>   Deploy specific component (dags|plugins|dbt|connections|minio|kafka|starrocks|flink)"
    echo "  --dry-run            Show what would be done without executing"
    echo "  --verbose            Enable verbose output"
    echo "  --build-flink        Build Flink JAR before deploying"
    echo "  --submit-jobs        Submit Flink jobs after deploying JAR"
    echo "  --restart-jobs       Stop existing Flink jobs before submitting new ones"
    echo ""
    echo "Examples:"
    echo "  $0 deploy test"
    echo "  $0 deploy prod --component dags"
    echo "  $0 deploy prod --component flink --build-flink --submit-jobs"
    echo "  $0 deploy prod --component minio"
    echo "  $0 deploy prod --component kafka"
    echo "  $0 deploy prod --component starrocks"
    echo "  $0 rollback test --component plugins"
    echo "  $0 status prod"
    exit 1
}

deploy() {
    local env=$1
    shift
    local extra_args=""

    while [[ $# -gt 0 ]]; do
        case $1 in
            --component)
                extra_args="$extra_args -e deploy_components=['$2']"
                shift 2
                ;;
            --dry-run)
                extra_args="$extra_args --check"
                shift
                ;;
            --verbose)
                extra_args="$extra_args -vvv"
                shift
                ;;
            --build-flink)
                extra_args="$extra_args -e flink_build_jar=true"
                shift
                ;;
            --submit-jobs)
                extra_args="$extra_args -e flink_submit_jobs=true"
                shift
                ;;
            --restart-jobs)
                extra_args="$extra_args -e flink_restart_jobs=true"
                shift
                ;;
            *)
                shift
                ;;
        esac
    done

    echo "Deploying to $env..."
    cd "$ANSIBLE_DIR"
    ansible-playbook deploy.yml \
        -i inventory/hosts.yml \
        -e target_env=$env \
        -e "s3_access_key=${s3_access_key:-}" \
        -e "s3_secret_key=${s3_secret_key:-}" \
        -e "postgres_airflow_password=${postgres_airflow_password:-Q1w2e3r+}" \
        -e "minio_password=${minio_password:-Q1w2e3r+}" \
        -e "starrocks_password=${starrocks_password:-Q1w2e3r+}" \
        $extra_args
}

rollback() {
    local env=$1
    shift
    local extra_args="-e do_rollback=true"

    while [[ $# -gt 0 ]]; do
        case $1 in
            --component)
                extra_args="$extra_args -e rollback_component=$2"
                shift 2
                ;;
            --dry-run)
                extra_args="-e do_rollback=false"
                shift
                ;;
            --verbose)
                extra_args="$extra_args -vvv"
                shift
                ;;
            *)
                shift
                ;;
        esac
    done

    echo "Rolling back $env..."
    cd "$ANSIBLE_DIR"
    ansible-playbook rollback.yml \
        -i inventory/hosts.yml \
        -e target_env=$env \
        $extra_args
}

status() {
    local env=$1
    echo "Checking status of $env..."
    cd "$ANSIBLE_DIR"
    ansible-playbook deploy.yml \
        -i inventory/hosts.yml \
        -e target_env=$env \
        --check \
        --diff
}

if [[ $# -lt 2 ]]; then
    usage
fi

COMMAND=$1
ENVIRONMENT=$2
shift 2

case $COMMAND in
    deploy)
        deploy $ENVIRONMENT "$@"
        ;;
    rollback)
        rollback $ENVIRONMENT "$@"
        ;;
    status)
        status $ENVIRONMENT "$@"
        ;;
    *)
        usage
        ;;
esac
