package com.datahub.authentication;

import com.datahub.authentication.token.TokenType;
import com.datahub.authentication.token.TokenService;
import com.datahub.metadata.authentication.AuthenticationContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;


@Slf4j
@RestController
public class AuthServiceController {

  private static final long SESSION_DURATION_MS = 2592000000L;  // 30 day session, by default.

  private static final String USER_URN_FIELD_NAME = "userUrn";

  // DataHub system internal Client ID
  private static final String CLIENT_ID_CONFIG_PATH = "systemClientId";

  @Inject
  TokenService _tokenService;

  @Inject
  ConfigProvider _configProvider;

  /**
   * Returns the currently authenticated actor urn for a given request, assuming the request
   * has been authenticated.
   */
  @GetMapping(value = "/getAuthenticatedActor", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> getAuthenticatedActor(final HttpEntity<String> httpEntity) {
    return CompletableFuture.supplyAsync(() -> new ResponseEntity<>(AuthenticationContext.getAuthentication().getAuthenticatedActor().toUrnStr(), HttpStatus.OK));
  }

  /**
   * Generates a JWT access token for as user UI session, provided a "userUrn" to generate the token for inside a JSON
   * POST body.
   *
   * Example Request:
   *
   * POST /generateSessionToken -H "Authorization: Basic <system-client-id>:<system-client-secret>"
   * {
   *   "userUrn": "urn:li:corpuser:johnsmith"
   * }
   *
   * Example Response:
   *
   * {
   *   "accessToken": "<the access token>"
   * }
   */
  @PostMapping(value = "/generateSessionToken", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> generateSessionToken(final HttpEntity<String> httpEntity) {
    String jsonStr = httpEntity.getBody();
    ObjectMapper mapper = new ObjectMapper();
    JsonNode bodyJson = null;
    try {
      bodyJson = mapper.readTree(jsonStr);
    } catch (JsonProcessingException e) {
      log.error(String.format("Failed to parse json %s", jsonStr));
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }
    if (bodyJson == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }
    /*
     * Extract userUrn field
     */
    JsonNode userUrn = bodyJson.get(USER_URN_FIELD_NAME);
    if (userUrn == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    final String actorId = AuthenticationContext.getAuthentication().getAuthenticatedActor().getId();
    return CompletableFuture.supplyAsync(() -> {
      // 1. Verify that only those authorized to generate a token are able to.
      if (isAuthorizedToGenerateSessionToken(actorId)) {
        try {
          // 2. Generate a new DataHub JWT
          final String token = _tokenService.generateAccessToken(
              TokenType.SESSION,
              new Actor(ActorType.CORP_USER, userUrn.asText()),
              SESSION_DURATION_MS);
          String responseBodyStr = new ObjectMapper().writeValueAsString(buildTokenResponse(token));
          return new ResponseEntity<>(responseBodyStr, HttpStatus.OK);
        } catch (Exception e) {
            log.error("Failed to generate json web token", e);
          return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
      throw HttpClientErrorException.create(HttpStatus.UNAUTHORIZED, "Unauthorized to perform this action.", new HttpHeaders(), null, null);
    });
  }

  // Currently only internal system is authorized to generate a user token!
  private boolean isAuthorizedToGenerateSessionToken(final String actorId) {
    final String systemClientId = (String) this._configProvider.getConfig(CLIENT_ID_CONFIG_PATH);
    return systemClientId.equals(actorId);
  }

  private JSONObject buildTokenResponse(final String token) {
    JSONObject json = new JSONObject();
    json.put("accessToken", token);
    return json;
  }
}
