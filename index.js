import express from 'express';
import bodyParser from 'body-parser';
import { fileURLToPath, pathToFileURL } from 'url';
import { dirname } from 'path';
import { Kafka } from "kafkajs";

const app = express(); // server initialize

//Kafka Integration
const kafka = new Kafka({
    clientId: 'node-producer',
    brokers: ['localhost:9092']
});
const producer = kafka.producer();
const consumer = kafka.consumer({groupId:'myKafkaGroup'});
let ReceivedMessage=[];


async function consumeMessages() {
await consumer.connect();
await consumer.subscribe({topic:'myKafkaSetup', fromBeginning:true});
await consumer.run({
     eachMessage: async({topic, partition, message}) =>{
         console.log(message.value.toString());
         ReceivedMessage.push(message.value.toString());
     }
})
}

consumeMessages().catch(console.error);

// variables and constants
const dirName = dirname(fileURLToPath(import.meta.url));

// middleware integration
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(dirname + '/app/public'));


app.get('/', (req, res) => {
    console.log("This is" +ReceivedMessage);
    res.render(dirName + '/app/src/kafka/UItoTest.ejs',{messages : ReceivedMessage});
});

app.get('/api/messages', (req, res) => {
    res.json({ messages: ReceivedMessage });
});


app.post('/message', async (req, res) => {
    const { message } = req.body;
    try {
        await producer.connect();

        await producer.send({
            topic: 'myKafkaSetup',
            messages: [
                { value: message }  // Send the message directly
            ],
        });

        await producer.disconnect();

        res.redirect('/');
    } catch (error) {
        console.error('Error in sending message to Kafka:', error);
        res.status(500).send('Error in sending message to Kafka');
    }
});

app.listen("3000", () => {
    console.log('Listening on port 3000');
});
