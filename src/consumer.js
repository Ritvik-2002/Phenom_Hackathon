const fs = require("fs/promises");
const path = require("path");
const { kafka, TOPIC, ensureTopic } = require("./config");

const OUTPUT_DIR = path.resolve(process.env.OUTPUT_DIR || "data/events");
const consumer = kafka.consumer({ groupId: process.env.GROUP_ID || "file-writer" });

async function main() {
  await ensureTopic();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      const day = event.event_time.slice(0, 10);
      const dir = path.join(OUTPUT_DIR, event.tenant_id);
      await fs.mkdir(dir, { recursive: true });
      // One JSON line per event: data/events/<tenant>/<yyyy-mm-dd>.ndjson
      await fs.appendFile(path.join(dir, `${day}.ndjson`), JSON.stringify(event) + "\n");
      console.log(`wrote ${event.event_id} -> ${dir}/${day}.ndjson`);
    },
  });
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

process.on("SIGINT", async () => {
  await consumer.disconnect();
  process.exit(0);
});
