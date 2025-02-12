import { Kafka } from "kafkajs";


const kafka = new Kafka({
    clientId:'my-app',
    brokers: ['localhost:9092']
});


const Consumer = kafka.consumer({groupId:'myKafkaGroup'});

const runConsumer = async () =>{
    Consumer.connect();
    console.log("Conneceted to kafka consumer");

await Consumer.subscribe({topic:'myKafkaSetup', fromBeginning:true});

await Consumer.run({
    eachMessage: async ({topic, partiation, message}) =>{
        rmessage=message.value.toString();
     console.log(`Received Message: ${message.value.toString()}`);
    },
})
};

runConsumer().catch(console.error);
