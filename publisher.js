require("dotenv").config();
const amqp = require("amqplib");
const express = require("express");
const bodyParser = require("body-parser");

const app = express();
app.use(bodyParser.json());

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const EXCHANGE_NAME = process.env.EXCHANGE_NAME;

async function publishMessage(routingKey, message) {
  try {
    const connection = await amqp.connect(RABBITMQ_URL,{heartbeat: 30});
    const channel = await connection.createChannel();

    await channel.assertExchange('notifications', 'fanout', { durable: true });
    const messageBuffer = Buffer.from(JSON.stringify(message));

    channel.publish(EXCHANGE_NAME, routingKey, messageBuffer);
    console.log(`📤 Sent to ${routingKey}:`, message);

    setTimeout(() => {
      connection.close();
    }, 500);
  } catch (error) {
    console.error("Error publishing message:", error);
  }
}

app.post("/publish", async (req, res) => {
  const { type, order_id, user_id, content, timestamp } = req.body;

  if (!type || !content) {
    return res.status(400).json({ error: "Invalid request. 'type' and 'content' are required." });
  }

  const message = { order_id, user_id, content, timestamp: timestamp || new Date().toISOString() };

  if (["EMAIL", "SMS", "FCM"].includes(type.toUpperCase())) {
    await publishMessage(type.toUpperCase(), message);
    return res.json({ status: "Message sent", type, message });
  } else {
    return res.status(400).json({ error: "Invalid type. Allowed: EMAIL, SMS, FCM." });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`🚀 Publisher service running on http://localhost:${PORT}`);
});
