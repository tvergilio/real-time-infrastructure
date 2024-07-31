package com.xdesign.flink.processing;

import com.xdesign.flink.model.GPT4Response;
import com.xdesign.flink.model.SlackMessage;
import okhttp3.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GPT4ProcessingFunction implements Serializable {

    static final String API_URL = "https://api.openai.com/v1/chat/completions";
    private final String apiKey;
    private transient OkHttpClient client;
    private transient ObjectMapper objectMapper;

    public GPT4ProcessingFunction() {
        this.apiKey = System.getenv("OPENAI_API_KEY");
        if (this.apiKey == null) {
            throw new IllegalStateException("OpenAI API key must be set in the environment variables");
        }
    }

    private void initialise() {
        if (client == null) {
            client = new OkHttpClient();
        }
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
    }

    public GPT4SentimentAccumulator map(List<SlackMessage> messages, long windowStart, long windowEnd) throws IOException {
        initialise();
        var prompt = createPrompt(messages, windowStart, windowEnd);
        var gpt4Response = callGPT4API(prompt);
        var resultText = gpt4Response.getChoices().get(0).getMessage().getContent().trim();
        return new GPT4SentimentAccumulator(resultText);
    }

    private String createPrompt(List<SlackMessage> messages, long windowStart, long windowEnd) {
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault());
        var startFormatted = formatter.format(Instant.ofEpochMilli(windowStart));
        var endFormatted = formatter.format(Instant.ofEpochMilli(windowEnd));

        return String.format(
                "Please analyse the following set of Slack messages and provide a summary of the overall sentiment. " +
                        "Indicate whether the sentiment is very positive, positive, neutral, negative, or very negative. " +
                        "Provide a summary paragraph that describes the overall sentiment, including the general tone.\n\n" +
                        "Window Start: %s\n" +
                        "Window End: %s\n" +
                        "Messages Processed: %d\n\nMessages:\n%s\n\n" +
                        "Then, provide the following structured summary:\n\n" +
                        "**Time Window:** %s - %s\n" +
                        "**Summary of Sentiment:**\n" +
                        "- **Overall Sentiment:** [Overall Sentiment]\n" +
                        "- **Most Positive Message:** [Most Positive Message]\n" +
                        "- **Most Negative Message:** [Most Negative Message]\n" +
                        "- **Message Count:** [Message Count]\n\n" +
                        "Finally, ensure the descriptive paragraph is included.",
                startFormatted, endFormatted, messages.size(),
                messages.stream().map(SlackMessage::getMessage).collect(Collectors.joining("\n- ", "- ", "")),
                startFormatted, endFormatted
        );
    }

    private GPT4Response callGPT4API(String prompt) throws IOException {
        initialise();

        var jsonRequest = objectMapper.writeValueAsString(Map.of(
                "model", "gpt-4o-mini",
                "messages", List.of(
                        Map.of("role", "system", "content", "You are a helpful assistant."),
                        Map.of("role", "user", "content", prompt)
                )
        ));

        // Create the request body
        var body = RequestBody.create(jsonRequest, MediaType.get("application/json; charset=utf-8"));

        // Build the request
        var request = new Request.Builder()
                .url(API_URL)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .post(body)
                .build();

        // Execute the request and handle the response
        try (Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                throw new IOException("Unexpected code " + response);
            }
            var responseBody = response.body().string();
            return objectMapper.readValue(responseBody, GPT4Response.class);
        }
    }
}
