const express = require('express');
const WebSocket = require('ws');
const kafka = require('kafka-node');

const app = express();
const port = 8080;

// Serve static files (HTML, CSS, JS)
app.use(express.static('public'));

// Start Express server
const server = app.listen(port, () => {
    console.log(`Server started on port ${port}`);
});

// Create WebSocket server
const wss = new WebSocket.Server({ server });

// Kafka consumer setup
const Consumer = kafka.Consumer;
const client = new kafka.KafkaClient({ kafkaHost: 'kafka:9092' });

const topics = [
    { topic: 'stanford_results', partition: 0 },
    { topic: 'gpt4_results', partition: 0 }
];

const options = {
    groupId: 'websocket-consumer-group',
    autoCommit: true,
    fromOffset: true
};

const consumer = new Consumer(client, topics, options);

consumer.on('error', (err) => {
    console.error('Kafka consumer error:', err);
});

consumer.on('message', (message) => {
    console.log('--- Received Kafka message ---');
    console.log(`Topic: ${message.topic}`);
    console.log(`Value: ${message.value}`);
    console.log('--- End of message ---');

    // Send the received message as raw text to the WebSocket client
    wss.clients.forEach(function each(client) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(message.value); // Send the raw JSON string
        }
    });
});

wss.on('connection', (ws) => {
    console.log('WebSocket client connected');

    // Rewind consumer to the earliest offset
    consumer.setOffset('stanford_results', 0, 0); // Rewind to offset 0 for the topic and partition
    consumer.setOffset('gpt4_results', 0, 0);

    ws.on('close', () => {
        console.log('WebSocket client disconnected');
    });
});
