const crypto = require("crypto");
const express = require("express");
const { kafka, TOPIC, TENANTS, ensureTopic } = require("./config");

const PORT = Number(process.env.PORT || 3000);
const producer = kafka.producer();
const app = express();
let producerConnected = false;
app.use(express.json());

// Authenticate tenant
app.use((req, res, next) => {
  const tenantId = TENANTS[req.header("x-api-key")];
  if (!tenantId) return res.status(401).json({ error: "invalid api key" });
  req.tenantId = tenantId;
  next();
});

app.post("/events", async (req, res) => {
  if (!producerConnected) {
    return res.status(503).json({ error: "event service is temporarily unavailable" });
  }

  const body = req.body;

  // Validate event
  if (!body || typeof body !== "object" || typeof body.event_type !== "string") {
    return res.status(400).json({ error: "event_type (string) is required" });
  }

  // Enrich with metadata
  const event = {
    event_id: `evt_${crypto.randomBytes(4).toString("hex")}`,
    tenant_id: req.tenantId,
    event_type: body.event_type,
    event_time: new Date().toISOString(),
    ...Object.fromEntries(
      Object.entries(body).filter(
        ([k]) => !["event_id", "tenant_id", "event_type", "event_time"].includes(k)
      )
    ),
  };

  try {
    // Key by tenant+user so a user's events stay ordered within a partition
    await producer.send({
      topic: TOPIC,
      messages: [
        { key: `${event.tenant_id}:${event.user_id || ""}`, value: JSON.stringify(event) },
      ],
    });
    res.status(202).json({ status: "accepted", event_id: event.event_id });
  } catch (err) {
    console.error("publish failed", err);
    res.status(502).json({ error: "failed to publish event" });
  }
});

async function main() {
  app.listen(PORT, () => console.log(`Event collector listening on :${PORT}`));
  await connectProducer();
}

async function connectProducer() {
  try {
    await ensureTopic();
    await producer.connect();
    producerConnected = true;
    console.log("Connected to Kafka");
  } catch (err) {
    console.error("Kafka connection failed; retrying in 5 seconds", err);
    setTimeout(connectProducer, 5000);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
