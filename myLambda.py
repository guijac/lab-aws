import json
import boto3
from datetime import datetime, timedelta
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# Configurar OpenTelemetry
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

otlp_exporter = OTLPSpanExporter(endpoint="your-otlp-endpoint")
span_processor = BatchSpanProcessor(otlp_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Inicializar o cliente DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('YourDynamoDBTableName')

def lambda_handler(event, context):
    for record in event['Records']:
        # Parse da mensagem do SQS
        message = json.loads(record['body'])
        transaction_id = message['transaction-id']
        state = message['state']
        timestamp = message['timestamp']

        # Calcular o TTL (1 mês a partir da data atual)
        ttl = int((datetime.utcnow() + timedelta(days=30)).timestamp())

        try:
            # Persistir o item no DynamoDB se o estado não existir
            table.put_item(
                Item={
                    'transaction-id': transaction_id,
                    'state': state,
                    'timestamp': timestamp,
                    'ttl': ttl
                },
                ConditionExpression='attribute_not_exists(#transaction_id) AND attribute_not_exists(#state)',
                ExpressionAttributeNames={
                    '#transaction_id': 'transaction-id',
                    '#state': 'state'
                }
            )

            # Registrar evento ao CloudWatch usando OpenTelemetry
            with tracer.start_as_current_span("persist_transaction") as span:
                span.set_attribute("transaction.id", transaction_id)
                span.set_attribute("transaction.state", state)
                span.set_attribute("transaction.timestamp", timestamp)
                span.add_event("Transaction persisted to DynamoDB", {
                    "transaction-id": transaction_id,
                    "state": state,
                    "timestamp": timestamp,
                    "ttl": ttl
                })

            print(f"Transaction {transaction_id} with state {state} persisted and sent to CloudWatch.")

        except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            print(f"Transaction {transaction_id} with state {state} already exists. Skipping persistence.")

    return {
        'statusCode': 200,
        'body': json.dumps('Process completed successfully!')
    }
