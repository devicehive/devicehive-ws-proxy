import { parentPort, workerData } from "node:worker_threads";
import { Kafka, Partitioners } from "kafkajs";

const { clientId, brokers } = workerData;
const kafka = new Kafka({
    clientId: clientId,
    brokers: brokers,
});

const producer = kafka.producer({
    createPartitioner: Partitioners.DefaultPartitioner,
});

await producer.connect();

parentPort.on("message", async (m) => {
    if (m === "terminate") {
        await producer.disconnect();
        process.exit();
    } else {
        const { topic, message } = m;

        if (topic && message) {
            await producer.send({
                topic: topic,
                messages: [{ value: message }],
            });
        }
    }
});

parentPort.postMessage("ready");

console.log(`Producer has been started`);
