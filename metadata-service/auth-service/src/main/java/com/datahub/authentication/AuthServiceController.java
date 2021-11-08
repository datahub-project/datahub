package com.datahub.authentication;

import com.datahub.authentication.token.DataHubAccessTokenType;
import com.datahub.authentication.token.DataHubTokenService;
import com.datahub.metadata.authentication.AuthenticationContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.concurrent.CompletableFuture;
import javax.inject.Inject;
import lombok.extern.slf4j.Slf4j;
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

  private static final long SESSION_DURATION_MS = 2592000000L;  // 30 day session

  // DataHub frontend Client ID
  private static final String CLIENT_ID_CONFIG_PATH = "system_client_id";

  // DataHub frontend Client secret
  private static final String CLIENT_SECRET_CONFIG_PATH = "system_client_secret";

  @Inject
  DataHubTokenService _tokenService;

  @Inject
  ConfigProvider _configProvider;

  /**
   * Returns the currently authenticated actor urn for a given request, assuming the request
   * has been authenticated.
   */
  @GetMapping(value = "/getAuthenticatedActor", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> getAuthenticatedActor(final HttpEntity<String> httpEntity) {
    return CompletableFuture.supplyAsync(() -> new ResponseEntity<>(AuthenticationContext.getAuthentication().getActorUrn(), HttpStatus.OK));
  }

  /**
   * Generates a JWT access token for as user UI session.
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
    JsonNode userUrn = bodyJson.get("userUrn");
    if (userUrn == null) {
      return CompletableFuture.completedFuture(new ResponseEntity<>(HttpStatus.BAD_REQUEST));
    }

    final String actorUrn = AuthenticationContext.getAuthentication().getActorUrn();
    return CompletableFuture.supplyAsync(() -> {
      // 1. Verify that only those authorized to generate a token are able to.
      if (isAuthorizedToGenerateSessionToken(actorUrn)) {
        // 2. Generate a new DataHub JWT
        final String token = _tokenService.generateAccessToken(DataHubAccessTokenType.SESSION, userUrn.asText(), SESSION_DURATION_MS);
        return new ResponseEntity<>(buildTokenResponse(token), HttpStatus.OK);
      }
      throw HttpClientErrorException.create(HttpStatus.UNAUTHORIZED, "Unauthorized to perform this action.", new HttpHeaders(), null, null);
    });
  }

  // Currently only internal system is authorized to generate a user token!
  private boolean isAuthorizedToGenerateSessionToken(final String actorUrn) {
    final String systemActorUrn = (String) this._configProvider.getConfigOrDefault(CLIENT_ID_CONFIG_PATH, Constants.SYSTEM_ACTOR);
    return systemActorUrn.equals(actorUrn);
  }

  private String buildTokenResponse(final String token) {
    return token;
  }
}
