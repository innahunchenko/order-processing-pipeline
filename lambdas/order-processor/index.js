const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const crypto = require("crypto");

const client = new DynamoDBClient({});
const TABLE_NAME = process.env.TABLE_NAME || "Orders";
const SHARD_COUNT = parseInt(process.env.SHARD_COUNT, 10) || 5;

exports.handler = async (event) => {
  const records = event?.Records ?? [];
  if (records.length === 0) {
    console.info("No messages received from SQS");
    return { batchItemFailures: [] };
  }

  console.info(`Received ${records.length} message(s) from SQS`);

  // Process all records concurrently
  const results = await Promise.allSettled(
    records.map((record) => processRecord(record))
  );

  // Collect failures for SQS partial batch response
  const failures = results
    .map((result, idx) => (result.status === "rejected" ? { itemIdentifier: records[idx].messageId } : null))
    .filter(Boolean);

  console.info("Batch processing complete", { failuresCount: failures.length });
  return { batchItemFailures: failures };
};

/**
 * Process a single SQS record
 * @param {Object} record
 */
async function processRecord(record) {
  const { messageId, body } = record;
  console.debug("Processing message", { messageId, body });

  let order;
  try {
    order = JSON.parse(body);
  } catch (err) {
    console.error("Invalid JSON in message body", { messageId, body, error: err });
    throw err;
  }

  const { pk, sk } = buildKeys(order);
  const putCommand = new PutItemCommand({
    TableName: TABLE_NAME,
    Item: {
      pk: { S: pk },
      sk: { S: sk },
      Email: { S: order.email },
      Price: { N: order.price.toString() },
      Products: { S: order.products.join(";") },
      Date: { S: order.date },
    },
  });

  await client.send(putCommand);
  console.info("Order saved to DynamoDB", { orderId: order.id, pk, sk });
}

/**
 * Build DynamoDB partition and sort keys with sharding
 * @param {Object} order
 * @returns {{ pk: string, sk: string }}
 */
function buildKeys(order) {
  const hash = crypto.createHash("sha256").update(order.id).digest("hex");
  const shardId = (parseInt(hash.slice(0, 8), 16) % SHARD_COUNT) + 1;

  return {
    pk: `${order.email}#${shardId}`,
    sk: `${order.date}#${order.id}`,
  };
}
