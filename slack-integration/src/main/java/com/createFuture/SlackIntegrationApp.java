package com.createFuture;

import com.slack.api.bolt.App;
import com.slack.api.bolt.AppConfig;
import com.slack.api.bolt.socket_mode.SocketModeApp;
import com.slack.api.model.event.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SlackIntegrationApp {

    public static void main(String[] args) throws Exception {
        Properties properties = loadProperties();
        String botToken = System.getenv("SLACK_BOT_TOKEN");
        String appToken = System.getenv("SLACK_APP_TOKEN");

        var app = configureSlackApp(botToken);
        var producer = configureKafkaProducer(properties);

        listenToSlackMessages(app, producer, properties);
        startSocketModeApp(appToken, app);
    }

    private static Properties loadProperties() throws Exception {
        var properties = new Properties();
        try (var input = SlackIntegrationApp.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Sorry, unable to find " + "application.properties");
            }
            properties.load(input);
        }
        return properties;
    }

    private static App configureSlackApp(String botToken) {
        var config = new AppConfig();
        config.setSingleTeamBotToken(botToken);
        return new App(config);
    }

    private static KafkaProducer<String, String> configureKafkaProducer(Properties properties) {
        var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("kafka.bootstrap.server"));
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(producerProps);
    }

    private static void listenToSlackMessages(App app, KafkaProducer<String, String> producer, Properties properties) {
        var channelId = System.getenv("SLACK_CHANNEL_ID");
        var topic = properties.getProperty("kafka.topic");

        app.event(MessageEvent.class, (payload, ctx) -> {
            if (channelId.equals(payload.getEvent().getChannel())) {
                var message = payload.getEvent().getText();
                var timestamp = payload.getEvent().getTs();
                var formattedMessage = String.format("Timestamp: %s, User: %s, Message: %s", timestamp, payload.getEvent().getUser(), message);
                producer.send(new ProducerRecord<>(topic, formattedMessage));
            }
            return ctx.ack();
        });
    }

    private static void startSocketModeApp(String appToken, App app) throws Exception {
        var socketModeApp = new SocketModeApp(appToken, app);
        socketModeApp.start();
    }
}
