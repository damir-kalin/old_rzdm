# Переменные для Kafka
# Функция для создания топика
KAFKA_BROKERS="127.0.0.1:9091"
KAFKA_USER="admin"
KAFKA_PASSWORD="Q1w2e3r+"

TOPICS=(
    "sys__nsi__esud_rzdm__organization__data"
    "sys__nsi__esud_rzdm__contractor__data"
    "sys__kuirzp__esud_rzdm__employee__data"
    "sys__asb__esud_rzdm__contractor_debt__data"
    "sys__buinu__esud_rzdm__form__data"
    "sys__isras__esud_rzdm__order__data"
    "sys__isras__esud_rzdm__movement__data"
    "sys__mis__esud_rzdm._data_99099.fdb.BUILDINGS"
    "sys__mis__esud_rzdm._data_99099.fdb.CHAIRS"
    "sys__mis__esud_rzdm._data_99099.fdb.ROOMS"
)

# Функция для создания топика
create_topic() {
    local topic_name=$1
    local partitions=$2
    local replication_factor=$3
    local kafka_brokers=$4
    local kafka_user=$5
    local kafka_password=$6

    echo "Creating topic: $topic_name (partitions=$partitions, replication=$replication_factor)"

    /opt/bitnami/kafka/bin/kafka-topics.sh \
        --bootstrap-server $kafka_brokers \
        --command-config <(cat <<EOF
        security.protocol=SASL_PLAINTEXT
        sasl.mechanism=PLAIN
        sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$kafka_user" password="$kafka_password";
        EOF
        ) \
        --create --topic $topic_name --partitions $partitions --replication-factor $replication_factor
    
    if [ $? -eq 0 ]; then
        echo "✓ Topic '$topic_name' created successfully"
    else
        echo "✗ Failed to create topic '$topic_name'"
    fi
}

# Создание топиков
for topic in "${TOPICS[@]}"; do
    create_topic $topic $partitions $replication_factor $KAFKA_BROKERS $KAFKA_USER $KAFKA_PASSWORD
done