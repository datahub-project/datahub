package com.datahub.authentication;

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
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;


@Slf4j
@RestController
public class TokenServiceController {

  // DataHub frontend Client ID
  private static final String CLIENT_ID_CONFIG_PATH = "system_client_id";

  // DataHub frontend Client secret
  private static final String CLIENT_SECRET_CONFIG_PATH = "system_client_secret";

  @Inject
  DataHubTokenService _tokenService;

  @Inject
  ConfigProvider _configProvider;

  @GetMapping(value = "/generateUserToken", produces = "application/json;charset=utf-8")
  CompletableFuture<ResponseEntity<String>> generateTokenForUser(final HttpEntity<String> httpEntity) {
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

    return CompletableFuture.supplyAsync(() -> {
      // 1. Verify that only those authorized to generate a token are able to.
      if (isAuthorizedToGenerateUserToken()) {
        // 2. Generate a new DataHub JWT
        final String token = _tokenService.generateToken(userUrn.asText());
        return new ResponseEntity<>(buildTokenResponse(token), HttpStatus.OK);
      }
      throw HttpClientErrorException.create(HttpStatus.UNAUTHORIZED, "Unauthorized to perform this action.", new HttpHeaders(), null, null);
    });
  }

  // Currently only internal system is authorized to generate a user token!
  private boolean isAuthorizedToGenerateUserToken() {
    final Authentication authentication = AuthenticationContext.getAuthentication();
    final String systemActorUrn = (String) this._configProvider.getConfigOrDefault(CLIENT_ID_CONFIG_PATH, Constants.SYSTEM_ACTOR);
    final String actorUrn = authentication.getActorUrn();
    return systemActorUrn.equals(actorUrn);
  }

  private String buildTokenResponse(final String token) {
    return token;
  }
}
