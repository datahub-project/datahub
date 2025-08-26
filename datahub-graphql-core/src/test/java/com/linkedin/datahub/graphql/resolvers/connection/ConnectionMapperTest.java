package com.linkedin.datahub.graphql.resolvers.connection;

import com.linkedin.datahub.graphql.generated.DataHubJsonConnection;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ConnectionMapperTest {
  @Test
  void testNoTokensPresent() throws Exception {
    String json = "{\"key\": \"value\"}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    Assert.assertEquals(json, result.getBlob());
  }

  @Test
  void testBotTokenObfuscation() throws Exception {
    String json = "{\"bot_token\": \"xoxb-1234567890-abcdefghijklmnop\"}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    String obfuscatedJson = result.getBlob();
    Assert.assertTrue(obfuscatedJson.contains("\"xo****nop\""));
  }

  @Test
  void testAppDetailsTokensObfuscation() throws Exception {
    String json =
        "{\"app_details\": {\"client_secret\": \"abcdefg1234567\", \"signing_secret\": \"hijklmn7654321\", \"verification_token\": \"opqrstu9876543\"}}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    String obfuscatedJson = result.getBlob();
    // client secret
    Assert.assertTrue(obfuscatedJson.contains("\"ab****567\""));
    // signing secret
    Assert.assertTrue(obfuscatedJson.contains("\"hi****321\""));
    // verification token
    Assert.assertTrue(obfuscatedJson.contains("\"op****543\""));
  }

  @Test
  void testAppConfigTokensObfuscation() throws Exception {
    String json =
        "{\"app_config_tokens\": {\"access_token\": \"xoxa-1234567890-abcdefghijklmnop\", \"refresh_token\": \"xoxr-1234567890-qrstuvwxyz\"}}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    String obfuscatedJson = result.getBlob();
    // access token
    Assert.assertTrue(obfuscatedJson.contains("\"xo****nop\""));
    // refresh token
    Assert.assertTrue(obfuscatedJson.contains("\"xo****xyz\""));
  }

  @Test
  void testAllTokenTypesObfuscation() throws Exception {
    String json =
        "{\"bot_token\": \"xoxb-1234567890-abcdefghijklmnop\", "
            + "\"app_details\": {\"client_secret\": \"abcdefg1234567\", \"signing_secret\": \"hijklmn7654321\", \"verification_token\": \"opqrstu9876543\"}, "
            + "\"app_config_tokens\": {\"access_token\": \"xoxa-1234567890-abcdefghijklmnop\", \"refresh_token\": \"xoxr-1234567890-qrstuvwxyz\"}}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    String obfuscatedJson = result.getBlob();
    // bot token
    Assert.assertTrue(obfuscatedJson.contains("\"xo****nop\""));
    // client secret
    Assert.assertTrue(obfuscatedJson.contains("\"ab****567\""));
    // signing secret
    Assert.assertTrue(obfuscatedJson.contains("\"hi****321\""));
    // verification token
    Assert.assertTrue(obfuscatedJson.contains("\"op****543\""));
    // access token
    Assert.assertTrue(obfuscatedJson.contains("\"xo****nop\""));
    // refresh token
    Assert.assertTrue(obfuscatedJson.contains("\"xo****xyz\""));
  }

  @Test
  void testInvalidJson() throws Exception {
    String json = "{invalid_json}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    Assert.assertEquals(json, result.getBlob());
  }

  @Test
  void testShortTokens() throws Exception {
    String json = "{\"bot_token\": \"12345\", \"app_details\": {\"client_secret\": \"ab\"}}";
    DataHubJsonConnection input = new DataHubJsonConnection(json);
    DataHubJsonConnection result = ConnectionMapper.tryObfuscateSlackBotTokens(input);
    String obfuscatedJson = result.getBlob();
    // bot token
    Assert.assertTrue(obfuscatedJson.contains("\"1****5\""));
    // client secret
    Assert.assertTrue(obfuscatedJson.contains("\"****b\""));
  }
}
