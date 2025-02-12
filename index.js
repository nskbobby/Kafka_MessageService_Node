import express from "express";
import bodyParser from "body-parser";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { sendMessage } from "./app/src/kafka/producer.js"; // Import producer function
import { ReceivedMessage } from "./app/src/kafka/consumer.js"; // Import consumer messages

const app = express();
const port = process.env.PORT || 3000;
const dirName = dirname(fileURLToPath(import.meta.url));

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.json());
app.use(express.static(dirName + "/app/public"));

// Routes
app.get("/", (req, res) => {
  console.log("Received Messages: ", ReceivedMessage);
  res.render(dirName + "/app/src/kafka/UItoTest.ejs", { messages: ReceivedMessage });
});

app.get("/api/messages", (req, res) => {
  res.json({ messages: ReceivedMessage });
});

app.post("/message", async (req, res) => {
  const { message } = req.body;
  try {
    await sendMessage(message);
    res.redirect("/");
  } catch (error) {
    console.error("Error sending message to Kafka:", error);
    res.status(500).send("Error in sending message to Kafka");
  }
});

app.listen(port, () => {
  console.log(`Listening on port ${port}`);
});
