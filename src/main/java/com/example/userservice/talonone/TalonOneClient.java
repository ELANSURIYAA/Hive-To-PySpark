package com.example.userservice.talonone;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.client.HttpStatusCodeException;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class TalonOneClient {
    @Value("${talonone.api.key}")
    private String apiKey;

    @Value("${talonone.api.url}")
    private String apiUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    public boolean registerUser(String userId, String email, String name) {
        String endpoint = apiUrl + "/v1/customers";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "ApiKey " + apiKey);

        Map<String, Object> body = new HashMap<>();
        body.put("integrationId", userId);
        body.put("attributes", Map.of("email", email, "name", name));

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);
        try {
            ResponseEntity<String> response = restTemplate.postForEntity(endpoint, request, String.class);
            return response.getStatusCode().is2xxSuccessful();
        } catch (HttpStatusCodeException ex) {
            log.error("Talon.One registration failed: {}", ex.getResponseBodyAsString());
            return false;
        } catch (Exception ex) {
            log.error("Talon.One registration exception: {}", ex.getMessage());
            return false;
        }
    }
}
