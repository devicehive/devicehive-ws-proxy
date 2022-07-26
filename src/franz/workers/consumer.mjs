import { parentPort, workerData } from "node:worker_threads";
import { Kafka, PartitionAssigners } from "kafkajs";

const { id, topic, groupId, clientId, brokers } = workerData;
const kafka = new Kafka({
    clientId: clientId,
    brokers: brokers,
});
const encoder = new TextDecoder("utf-8");
const config = {};

config.groupId = groupId;
config.partitionAssigners = [PartitionAssigners.roundRobin];

const consumer = kafka.consumer(config);

await consumer.connect();

await consumer.subscribe({ topics: [topic] });

await consumer.run({
    eachMessage: async ({ message }) => {
        const decodedMessage = encoder.decode(message.value);
        parentPort.postMessage(decodedMessage);
    },
});

parentPort.on("message", async (message) => {
    if (message === "terminate") {
        await consumer.disconnect();
        process.exit();
    }
});

parentPort.postMessage("ready");

console.log(`Consumer ${id} for topic ${topic} has been started`);
