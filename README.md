# order-processing-pipeline
# order-processing-pipeline
Order Processing Pipeline

This project implements a **serverless order processing pipeline** on AWS.  
It ingests orders from a CSV file via API Gateway, processes them with AWS Lambda, and stores them in DynamoDB through an event-driven pipeline.

## Architecture

The solution consists of the following components:

- **API Gateway** – exposes a `POST /upload` endpoint for uploading CSV files (orders.scv).
- **CSV Parser Lambda (`csv-parser-lambda`)** – parses the uploaded CSV file and sends individual order records as messages to SQS.
- **Amazon SQS (OrdersQueue)** – decouples ingestion and processing. Messages that fail processing after two attempts are redirected to a Dead Letter Queue (OrdersDLQ).
- **Order Processor Lambda (`order-processor-lambda`)** – consumes messages from SQS and writes order records into DynamoDB.
  - Enabled with *ReportBatchItemFailures* for partial failure handling.
- **Amazon DynamoDB (Orders table)** – stores processed orders with a composite primary key (`pk`, `sk`).

## Workflow

1. Client uploads a CSV file via the `POST /upload` API Gateway endpoint.
2. The **CSV Parser Lambda** reads the file, parses each row, and sends messages to SQS.
3. **Order Processor Lambda** is triggered by SQS and stores the parsed order data in DynamoDB.
4. Failed messages are redirected to the DLQ for troubleshooting.

## Deployment

The infrastructure is fully defined in **AWS CloudFormation** (`orders-processing-pipeline.yaml`):  

- Deploy via AWS Management Console or AWS CLI.  
- Lambda function code is stored in S3 and referenced in the template.

