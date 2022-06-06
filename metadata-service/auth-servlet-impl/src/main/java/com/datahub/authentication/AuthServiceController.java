package com.datahub.authentication;

import com.datahub.authentication.token.TokenType;
import com.datahub.authentication.token.StatelessTokenService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;


@Slf4j
@RestController
public class AuthServiceController {

  private static final String USER_ID_FIELD_NAME = "userId";
  private static final String ACCESS_TOKEN_FIELD_NAME = "accessToken";

  @Inject
  StatelessTokenService _statelessTokenService;

  @Inject
  Authentication _systemAuthentication;

  @Inject
  ConfigurationProvider _configProvider;

  /**
   * Generates a JWT access token for as user UI session, provided a unique "user id" to generate the token for inside a JSON
   * POST body.
   *
   * Example Request:
   *
   * POST /generateSessionToken -H "Authorization: Basic <system-client-id>:<system-client-secret>"
   * {
   *   "userId": "datahub"
   * }
   *
   * Example Response:
   *
   * {
   *   "accessToken": "<the access token>"
   * }
   */
  @PostMapping(value = "/generateSessionTokenForUser", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> generateSessionTokenForUser(final HttpEntity<String> httpEntity) {
    String jsonStr = httpEntity.getBody();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode bodyJson = null;
    try {
      bodyJson = mapper.readTree(jsonStr);
    } catch (JsonProcessingException e) {
      log.error(String.format("Failed to parse json while attempting to generate session token %s", jsonStr));
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }
    if (bodyJson == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }
    /*
     * Extract userId field
     */
    JsonNode userId = bodyJson.get(USER_ID_FIELD_NAME);
    if (userId == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    log.debug(String.format("Attempting to generate session token for user %s", userId.asText()));
    final String actorId = AuthenticationContext.getAuthentication().getActor().getId();
    return CompletableFuture.supplyAsync(() -> {
      // 1. Verify that only those authorized to generate a token (datahub system) are able to.
      if (isAuthorizedToGenerateSessionToken(actorId)) {
        try {
          // 2. Generate a new DataHub JWT
          final String token = _statelessTokenService.generateAccessToken(
              TokenType.SESSION,
              new Actor(ActorType.USER, userId.asText()),
              _configProvider.getAuthentication().getSessionTokenDurationMs());
          return new ResponseEntity<>(buildTokenResponse(token), HttpStatus.OK);
        } catch (Exception e) {
          log.error("Failed to generate session token for user", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
      throw HttpClientErrorException.create(HttpStatus.UNAUTHORIZED, "Unauthorized to perform this action.", new HttpHeaders(), null, null);
    });
  }

  // Currently only internal system is authorized to generate a token on behalf of a user!
  private boolean isAuthorizedToGenerateSessionToken(final String actorId) {
    // Verify that the actor is an internal system caller.
    final String systemClientId = _systemAuthentication.getActor().getId();
    return systemClientId.equals(actorId);
  }

  private String buildTokenResponse(final String token) {
    JSONObject json = new JSONObject();
    json.put(ACCESS_TOKEN_FIELD_NAME, token);
    return json.toString();
  }
}
