import { randomUUID } from "crypto";
import { MongoClient, ServerApiVersion } from "mongodb";

interface Measurement {
  metadata: {
    deviceType: "station";
    stationId: string;
    connectorId: number;
    sessionId: string;
    locationId: string;
    networkId: string;
    measurementType:
      | "currentImport"
      | "powerActiveImport"
      | "energyActiveImportRegister";
  };
  timestamp: Date; // Changed to Date type
  value: number;
  unit: "A" | "kW" | "Wh" | "kWh";
}

async function generateFakeData(
  numStations: number = 5000,
  numSessionsPerStation: number = 100,
  minSessionDurationMinutes: number = 120,
  maxSessionDurationMinutes: number = 240,
  mongoUri: string,
  dbName: string,
  collectionName: string
): Promise<void> {
  const stationIds: string[] = Array.from({ length: numStations }, () =>
    randomUUID().replace(/-/g, "").substring(0, 10)
  );
  const networkId: string = randomUUID().replace(/-/g, "").substring(0, 10);

  console.log(
    `Generating data for ${numStations} stations and writing to MongoDB...`
  );

  const client = new MongoClient(mongoUri, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    },
  });

  try {
    await client.connect();
    console.log("Connected successfully to server");
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const batchSize = 1000; // Adjust batch size as needed
    let bulkOperations = [];

    for (
      let stationIndex = 0;
      stationIndex < stationIds.length;
      stationIndex++
    ) {
      const stationId = stationIds[stationIndex];
      console.log(
        `  Generating data for station ${
          stationIndex + 1
        }/${numStations} (ID: ${stationId})...`
      );

      for (
        let sessionIndex = 0;
        sessionIndex < numSessionsPerStation;
        sessionIndex++
      ) {
        const sessionId: string = randomUUID()
          .replace(/-/g, "")
          .substring(0, 10);
        const connectorId: number = Math.floor(Math.random() * 4) + 1; // 1-4 connectors
        const locationId: string = randomUUID()
          .replace(/-/g, "")
          .substring(0, 10);

        // Ensure session lasts at least an hour
        const sessionDurationMinutes =
          Math.floor(
            Math.random() *
              (maxSessionDurationMinutes - minSessionDurationMinutes + 1)
          ) + minSessionDurationMinutes;

        let startTime: Date = new Date(
          Date.now() - Math.random() * sessionDurationMinutes * 60 * 1000
        );
        let currentTime: Date = startTime;

        // Simulate charging session with varying power levels
        const powerLevels: number[] = [
          Math.random() * (8 - 6) + 6, // Initial power (kW)
          Math.random() * (7.6 - 7) + 7, // Increased power (kW)
          Math.random() * (6 - 5) + 5, // Reduced power (kW)
          0.0, // Charging finished (kW)
        ];
        const powerLevelDurations: number[] = Array.from(
          { length: powerLevels.length - 1 },
          () => Math.floor(Math.random() * (15 - 5) + 5)
        ).concat([0]); // Minutes for each power level
        let energyRegister: number = 0.0; // Initial energy register value (Wh)

        // Ensure measurements come in every minute
        while (
          currentTime <
          new Date(startTime.getTime() + sessionDurationMinutes * 60 * 1000)
        ) {
          const timestamp: Date = currentTime; // Use Date object directly

          // Calculate current based on power (approximate)
          let current: number;
          if (powerLevels.length > 0) {
            const powerLevel =
              powerLevels[
                Math.min(
                  Math.floor(currentTime.getMinutes() / 15),
                  powerLevels.length - 1
                )
              ]; // Cycle through power levels every 15 minutes
            current = Math.min(Math.max((powerLevel * 1000) / 240, 0), 32); // Assuming 240V, clamp to 32A
            current = parseFloat(current.toFixed(2));

            // Generate data for each measurement type
            const currentImportRecord = createMeasurementRecord(
              stationId,
              connectorId,
              sessionId,
              locationId,
              networkId,
              "currentImport",
              timestamp,
              current,
              "A"
            );

            const powerValue: number = parseFloat(powerLevel.toFixed(2));
            const powerUnit: "kW" = "kW";
            const powerActiveImportRecord = createMeasurementRecord(
              stationId,
              connectorId,
              sessionId,
              locationId,
              networkId,
              "powerActiveImport",
              timestamp,
              powerValue,
              powerUnit
            );

            // Calculate energy consumption (Wh)
            const energyIncrement: number = (powerLevel * 1000 * 1) / 60; // Wh per minute
            energyRegister += energyIncrement;
            const energyRegisterValue: number = parseFloat(
              energyRegister.toFixed(2)
            );
            const energyRegisterUnit: "Wh" = "Wh";
            const energyActiveImportRegisterRecord = createMeasurementRecord(
              stationId,
              connectorId,
              sessionId,
              locationId,
              networkId,
              "energyActiveImportRegister",
              timestamp,
              energyRegisterValue,
              energyRegisterUnit
            );

            // Add to bulk operations
            bulkOperations.push({
              insertOne: { document: currentImportRecord },
            });
            bulkOperations.push({
              insertOne: { document: powerActiveImportRecord },
            });
            bulkOperations.push({
              insertOne: { document: energyActiveImportRegisterRecord },
            });

            if (bulkOperations.length >= batchSize) {
              await collection.bulkWrite(bulkOperations);
              console.log(`Inserted ${bulkOperations.length} documents.`);
              bulkOperations = []; // Reset bulk operations
            }
          }

          currentTime = new Date(currentTime.getTime() + 60 * 1000); // Increment time by 1 minute
        }
      }
    }

    // Insert any remaining documents
    if (bulkOperations.length > 0) {
      await collection.bulkWrite(bulkOperations);
      console.log(`Inserted remaining ${bulkOperations.length} documents.`);
    }

    console.log("Successfully wrote to the database");
  } finally {
    await client.close();
    console.log("Disconnected from MongoDB");
  }
}

function createMeasurementRecord(
  stationId: string,
  connectorId: number,
  sessionId: string,
  locationId: string,
  networkId: string,
  measurementType:
    | "currentImport"
    | "powerActiveImport"
    | "energyActiveImportRegister",
  timestamp: Date, // Changed to Date type
  value: number,
  unit: "A" | "kW" | "Wh" | "kWh"
): Measurement {
  return {
    metadata: {
      deviceType: "station",
      stationId: stationId,
      connectorId: connectorId,
      sessionId: sessionId,
      locationId: locationId,
      networkId: networkId,
      measurementType: measurementType,
    },
    timestamp: timestamp,
    value: value,
    unit: unit,
  };
}

async function main(): Promise<void> {
  const numStations: number = 3000;
  const numSessionsPerStation: number = 50;
  const mongoUri: string = "mongodb://localhost:27017/"; // Replace with your MongoDB connection URI
  const dbName: string = "sessions";
  const collectionName: string = "meterValues";

  await generateFakeData(
    numStations,
    numSessionsPerStation,
    120,
    240,
    mongoUri,
    dbName,
    collectionName
  );
}

main().catch(console.error);
