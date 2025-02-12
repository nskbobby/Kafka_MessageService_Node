import { Kafka } from "kafkajs";

const kafka_PORT = process.env.PORT || 9092;
const kafka = new Kafka({
    clientId:'my-app',
    brokers: [`localhost:${kafka_PORT}`]
});


const Consumer = kafka.consumer({groupId:'myKafkaGroup'});
export let ReceivedMessage = [];

const runConsumer = async () =>{
    Consumer.connect();
    console.log("Conneceted to kafka consumer");

await Consumer.subscribe({topic:'myKafkaSetup', fromBeginning:true});

await Consumer.run({
    eachMessage: async ({topic, partiation, message}) =>{
      console.log(`Received Message: ${message.value.toString()}`);
      ReceivedMessage.push(message.value.toString());
    },
})
};

runConsumer().catch(console.error);
