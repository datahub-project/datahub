package com.datahub.authentication.authenticator;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authentication.token.StatefulTokenService;
import com.datahub.authentication.token.TokenType;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import java.util.Collections;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datahub.authentication.AuthenticationConstants.*;
import static com.datahub.authentication.authenticator.DataHubTokenAuthenticator.*;
import static com.datahub.authentication.token.TokenClaims.*;
import static org.testng.Assert.*;


public class DataHubTokenAuthenticatorTest {

    private static final String TEST_SIGNING_KEY = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI94=";
    private static final String TEST_SALT = "WnEdIeTG/VVCLQqGwC/BAkqyY0k+H8NEAtWGejrBI93=";

    final EntityService mockService = Mockito.mock(EntityService.class);
    final StatefulTokenService statefulTokenService = new StatefulTokenService(TEST_SIGNING_KEY, "HS256", null, mockService, TEST_SALT);

    @Test
    public void testInit() {
        final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
        AuthenticatorContext authenticatorContext =
            new AuthenticatorContext(ImmutableMap.of(ENTITY_SERVICE, mockService, TOKEN_SERVICE, statefulTokenService));
        assertThrows(() -> authenticator.init(null, authenticatorContext));
        assertThrows(() -> authenticator.init(Collections.emptyMap(), authenticatorContext));
        assertThrows(() -> authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY,
            SIGNING_ALG_CONFIG_NAME, "UNSUPPORTED_ALG"), authenticatorContext));
        assertThrows(() -> authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY,
            SIGNING_ALG_CONFIG_NAME, "HS256"), null));

        // Correct configs provided.
        authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALT_CONFIG_NAME,
                        TEST_SALT, SIGNING_ALG_CONFIG_NAME, "HS256"), authenticatorContext);
    }

    @Test
    public void testAuthenticateFailureMissingAuthorizationHeader() {
        final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();

        authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALT_CONFIG_NAME,
                        TEST_SALT, SIGNING_ALG_CONFIG_NAME, "HS256"),
            new AuthenticatorContext(ImmutableMap.of(ENTITY_SERVICE, mockService, TOKEN_SERVICE, statefulTokenService)));

        final AuthenticationRequest context = new AuthenticationRequest(Collections.emptyMap());
        assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
    }

    @Test
    public void testAuthenticateFailureMissingBearerCredentials() {
        final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
        authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALT_CONFIG_NAME,
                        TEST_SALT, SIGNING_ALG_CONFIG_NAME, "HS256"),
            new AuthenticatorContext(ImmutableMap.of(ENTITY_SERVICE, mockService, TOKEN_SERVICE, statefulTokenService)));

        final AuthenticationRequest context = new AuthenticationRequest(
                ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Basic username:password")
        );
        assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
    }

    @Test
    public void testAuthenticateFailureInvalidToken() {
        final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();

        authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALT_CONFIG_NAME,
                        TEST_SALT, SIGNING_ALG_CONFIG_NAME, "HS256"),
            new AuthenticatorContext(ImmutableMap.of(ENTITY_SERVICE, mockService, TOKEN_SERVICE, statefulTokenService)));

        final AuthenticationRequest context = new AuthenticationRequest(
                ImmutableMap.of(AUTHORIZATION_HEADER_NAME, "Bearer someRandomToken")
        );
        assertThrows(AuthenticationException.class, () -> authenticator.authenticate(context));
    }

    @Test
    public void testAuthenticateSuccess() throws Exception {
        PathSpecBasedSchemaAnnotationVisitor.class.getClassLoader()
                .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
        final ConfigEntityRegistry configEntityRegistry = new ConfigEntityRegistry(
                DataHubTokenAuthenticatorTest.class.getClassLoader().getResourceAsStream("test-entity-registry.yaml"));
        final AspectSpec keyAspectSpec = configEntityRegistry.getEntitySpec(Constants.ACCESS_TOKEN_ENTITY_NAME).getKeyAspectSpec();
        Mockito.when(mockService.getKeyAspectSpec(Mockito.eq(Constants.ACCESS_TOKEN_ENTITY_NAME))).thenReturn(keyAspectSpec);
        Mockito.when(mockService.exists(Mockito.any(Urn.class))).thenReturn(true);

        final DataHubTokenAuthenticator authenticator = new DataHubTokenAuthenticator();
        authenticator.init(ImmutableMap.of(SIGNING_KEY_CONFIG_NAME, TEST_SIGNING_KEY, SALT_CONFIG_NAME,
                        TEST_SALT, SIGNING_ALG_CONFIG_NAME, "HS256"),
            new AuthenticatorContext(ImmutableMap.of(ENTITY_SERVICE, mockService, TOKEN_SERVICE, statefulTokenService)));

        final Actor datahub = new Actor(ActorType.USER, "datahub");
        final String validToken = authenticator._statefulTokenService.generateAccessToken(
                TokenType.PERSONAL,
                datahub,
                "some token",
                "A token description",
                datahub.toUrnStr()
        );

        final String authorizationHeaderValue = String.format("Bearer %s", validToken);
        final AuthenticationRequest context = new AuthenticationRequest(
                ImmutableMap.of(AUTHORIZATION_HEADER_NAME, authorizationHeaderValue)
        );

        final Authentication authentication = authenticator.authenticate(context);

        // Validate the resulting authentication object
        assertNotNull(authentication);
        assertEquals(authentication.getActor().getType(), ActorType.USER);
        assertEquals(authentication.getActor().getId(), "datahub");
        assertEquals(authentication.getCredentials(), authorizationHeaderValue);

        Map<String, Object> claimsMap = authentication.getClaims();
        assertEquals(claimsMap.get(TOKEN_VERSION_CLAIM_NAME), 2);
        assertEquals(claimsMap.get(TOKEN_TYPE_CLAIM_NAME), "PERSONAL");
        assertEquals(claimsMap.get(ACTOR_TYPE_CLAIM_NAME), "USER");
        assertEquals(claimsMap.get(ACTOR_ID_CLAIM_NAME), "datahub");
    }
/*
    @Test
    public void testAccessTokens() {
        final String salt = "0lib4AggPIuXeBvEBgvotJKg5IgohtDQwqXkMe+Lmao=";

        final String expected_id = "<expected value>";
        final String access_token = "eyJhbGciOiJIUzI1NiJ9.eyJhY3RvclR5cGUiOiJVU0VSIiwiYWN0b3JJZCI6ImFkbWluIiwidHlwZS"
            + "I6IlBFUlNPTkFMIiwidmVyc2lvbiI6IjIiLCJqdGkiOiIyM2U5Y2UyMS1kNWYzLTQ3ODgtYTFjNy05NTRmZjI0NDI4NGQiLCJzdW"
            + "IiOiJhZG1pbiIsImV4cCI6MTY3MjIyNDAwOSwiaXNzIjoiZGF0YWh1Yi1tZXRhZGF0YS1zZXJ2aWNlIn0.WyYiNGhJ50xFzw1x1"
            + "8yUUJqKkkalcPl039VHYZBah3s";

        final byte[] saltingKeyBytes = salt.getBytes();
        final byte[] inputBytes = access_token.getBytes();
        final byte[] concatBytes = ArrayUtils.addAll(inputBytes, saltingKeyBytes);
        final byte[] bytes = DigestUtils.sha256(concatBytes);
        String result = Base64.getEncoder().encodeToString(bytes);

        assertEquals(result, expected_id);
    }*/
}
