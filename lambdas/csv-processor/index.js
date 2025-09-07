const Busboy = require("busboy");
const csvParser = require("csv-parser");
const { v4: uuidv4 } = require("uuid");
const { SQSClient, SendMessageBatchCommand } = require("@aws-sdk/client-sqs");

const sqsClient = new SQSClient({});
const QUEUE_URL = process.env.SQS_QUEUE_URL;
const BATCH_SIZE = 10;

/**
 * Parse CSV from multipart/form-data event
 */
const parseCsvFromEvent = (event) => {
  return new Promise((resolve, reject) => {
    if (!event.body) {
      return reject(new Error("No file uploaded"));
    }

    const bodyBuffer = event.isBase64Encoded
      ? Buffer.from(event.body, "base64")
      : Buffer.from(event.body, "utf-8");

    const headers = Object.fromEntries(
      Object.entries(event.headers).map(([k, v]) => [k.toLowerCase(), v])
    );

    const orders = [];
    const busboy = Busboy({ headers });

    busboy.on("file", (_, fileStream) => {
      fileStream
        .pipe(csvParser({ separator: "," }))
        .on("data", (row) => orders.push(row))
        .on("error", reject);
    });

    busboy.on("finish", () => resolve(orders));
    busboy.on("error", reject);

    busboy.end(bodyBuffer);
  });
};

/**
 * Send orders to SQS in batches
 */
const sendOrdersToSqs = async (orders) => {
  for (let i = 0; i < orders.length; i += BATCH_SIZE) {
    const batch = orders.slice(i, i + BATCH_SIZE);
    const batchNumber = i / BATCH_SIZE + 1;

    const entries = batch.map((order) => {
      const id = uuidv4();
      return {
        Id: id,
        MessageBody: JSON.stringify({
          id,
          email: order.email,
          price: parseFloat(order.price),
          products: order.products ? order.products.split(";") : [],
          date: order.date,
        }),
      };
    });

    const command = new SendMessageBatchCommand({
      QueueUrl: QUEUE_URL,
      Entries: entries,
    });

    const result = await sqsClient.send(command);

    console.log(`Batch ${batchNumber} sent. Successful: ${result.Successful.length}, Failed: ${result.Failed?.length || 0}`);

    if (result.Failed && result.Failed.length > 0) {
      result.Failed.forEach(({ Id, Message }) => console.error(`Failed message ${Id}: ${Message}`));
    }
  }
};

/**
 * Lambda handler
 */
exports.handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  let orders;
  try {
    orders = await parseCsvFromEvent(event);
  } catch (err) {
    console.error("CSV parsing error:", err);
    return { statusCode: 400, body: JSON.stringify({ error: err.message }) };
  }

  console.log(`Parsed ${orders.length} orders`);

  try {
    await sendOrdersToSqs(orders);
  } catch (err) {
    console.error("SQS error:", err);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }

  return {
    statusCode: 200,
    body: JSON.stringify({ message: `Processed ${orders.length} orders`, orders }),
  };
};
