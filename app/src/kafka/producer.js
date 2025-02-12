import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'node-producer',
    brokers: ['localhost:9092']
});

const Producer = kafka.producer();

export async function sendMessage (message) {
    try{
await Producer.connect();
console.log("connected to kafka producer and message is " + `${message}`);

await Producer.send({
    topic:'myKafkaSetup',
    messages: [{value:message}]
});

console.log("Message send");

await Producer.disconnect();
} catch(error){
    console.log("error while sending message to Kafka" +error);
    throw error;
}
}