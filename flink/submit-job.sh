#!/bin/bash

JM_PORT="${FLINK_REST_PORT:-8081}"
JM_ADDRESS="${FLINK_REST_ADDRESS:-flink-jobmanager}"
CLASSES=(
  "com.example.streaming.OrganizationsPipeline"
  "com.example.streaming.ContractorsPipeline"
  "com.example.streaming.ContractorDebtPipeline"
  "com.example.streaming.EmployeePipeline"
  "com.example.streaming.FormPipeline"
  "com.example.streaming.KpiPipeline"
)

for CLASS in "${CLASSES[@]}"; do
  echo "Submitting ${CLASS}..."
  
  flink run \
    -Drest.address=${JM_ADDRESS} \
    -Drest.port=${JM_PORT} \
    -m "${JM_ADDRESS}:${JM_PORT}" \
    -d \
    -c ${CLASS} \
    -p 1 \
    /opt/usrlib/streaming-data-pipeline-1.0-SNAPSHOT.jar
  
  EXIT_CODE=$?
  if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ ${CLASS} job submitted successfully"
  else
    echo "❌ Failed to submit ${CLASS} job (exit code: ${EXIT_CODE})"
  fi
  echo ""
done

echo ""
echo "All jobs submitted. Check Flink Web UI at http://${JM_ADDRESS}:${JM_PORT} for job status"