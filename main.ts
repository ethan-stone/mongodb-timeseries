import * as dotenv from "dotenv";
import { MongoClient } from "mongodb";
import { randomUUID } from "crypto";

dotenv.config();

const mongoUrl = process.env.DATABASE_URL;

if (!mongoUrl) throw new Error("Missing mongo url");

const mongoClient = new MongoClient(mongoUrl);

type Log = {
  metadata: { chargingStationId: string };
  timestamp: Date;
  log: Record<string, any>;
};

async function setTimeoutPromise(timeout: number) {
  return new Promise((resolve) => {
    setTimeout(resolve, timeout);
  });
}

async function main() {
  const db = mongoClient.db("logs");
  const collection = db.collection<Log>("logs");

  const logs: Log[] = [];
  for (let i = 0; i < 100; i++) {
    const log = {
      metadata: { chargingStationId: randomUUID() },
      timestamp: new Date(),
      log: {
        occpMessageType: "RemoteStartTransaction",
        ocppMessageId: randomUUID(),
        ocppMessagePayload: {}
      }
    };
    logs.push(log);
    console.log(log);

    await setTimeoutPromise(50);
  }

  await collection.insertMany(logs);

  console.log("Logs inserted");
}

main();
