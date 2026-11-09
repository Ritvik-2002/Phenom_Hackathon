const { Kafka } = require("kafkajs");

const BROKERS = (process.env.KAFKA_BROKERS || "localhost:9092").split(",");
const TOPIC = process.env.KAFKA_TOPIC || "user-events";
const PARTITIONS = Number(process.env.KAFKA_PARTITIONS || 4);

// API key -> tenant id (stand-in for a real tenant store)
const TENANTS = {
  "key-123": "tenant_123",
  "key-456": "tenant_456",
};

const kafka = new Kafka({ clientId: "phenom", brokers: BROKERS });

async function ensureTopic() {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic: TOPIC, numPartitions: PARTITIONS }],
  });
  await admin.disconnect();
}

module.exports = { kafka, TOPIC, TENANTS, ensureTopic };
