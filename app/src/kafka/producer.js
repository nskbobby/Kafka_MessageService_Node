import { Kafka } from "kafkajs";

const kafka = new Kafka({
    clientId: 'node-producer',
    brokers: ['localhost:9092']
});

const Producer = kafka.producer();

const sendMessage =async () =>{
await Producer.connect();
console.log("connected to kafka producer");
}

await Producer.send({
    topic:'myKafkaSetup',
    messages: [{value:'hello world!!'}]
});

console.log("Message send");

await Producer.disconnect();

sendMessage.catch(error);