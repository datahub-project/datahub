package com.datahub.graphql;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Writer;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testng.annotations.Test;

/** HTTP-level verification of the buffered, streamed, and clean pre-commit failure paths. */
public class GraphQLResponseHttpIntegrationTest {

  @Test
  public void testBufferedAndStreamingResponsesOverHttp() throws Exception {
    boolean[] writeFinished = {false};
    MockMvc mvc = mvc(new ObjectMapper(), writeFinished);

    mvc.perform(get("/response/buffered"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(content().json("{\"data\":{\"value\":\"é汉😀\"}}"));

    assertFalse(writeFinished[0], "buffered response must not use the streaming callback");

    MvcResult streaming =
        mvc.perform(get("/response/streamed")).andExpect(request().asyncStarted()).andReturn();
    mvc.perform(asyncDispatch(streaming))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(content().json("{\"data\":{\"value\":\"é汉😀\"}}"));

    assertTrue(writeFinished[0], "streaming response must complete its write callback");
  }

  @Test
  public void testUncommittedStreamingFailureBecomesCleanServiceUnavailable() throws Exception {
    ObjectMapper failingMapper = mock(ObjectMapper.class);
    doThrow(new JsonProcessingException("cannot serialize") {})
        .when(failingMapper)
        .writeValue(any(Writer.class), any());
    boolean[] writeFinished = {true};
    MockMvc mvc = mvc(failingMapper, writeFinished);

    MvcResult streaming =
        mvc.perform(get("/response/streamed")).andExpect(request().asyncStarted()).andReturn();
    String body =
        mvc.perform(asyncDispatch(streaming))
            .andExpect(status().isServiceUnavailable())
            .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
            .andReturn()
            .getResponse()
            .getContentAsString();

    assertEquals(body, "{\"error\":\"Failed to serialize GraphQL response\"}");
    assertFalse(writeFinished[0], "failed streaming response must report unsuccessful completion");
  }

  private static MockMvc mvc(ObjectMapper streamingMapper, boolean[] writeFinished) {
    HttpMessageConverter<?>[] converters = {
      new GraphQLResponseBodyConverter(streamingMapper, null),
      new StringHttpMessageConverter(),
      new MappingJackson2HttpMessageConverter()
    };
    return MockMvcBuilders.standaloneSetup(new ResponseController(writeFinished))
        .setMessageConverters(converters)
        .build();
  }

  /**
   * Subclasses {@link GraphQLController} so standalone MockMvc inherits the controller-local
   * {@code @ExceptionHandler}s. Standalone setup has no advice chain, and a test-only advice would
   * not reproduce production precedence over GlobalControllerExceptionHandler.
   */
  @RestController
  @RequestMapping("/response")
  private static class ResponseController extends GraphQLController {
    private final boolean[] writeFinished;

    ResponseController(boolean[] writeFinished) {
      this.writeFinished = writeFinished;
    }

    @GetMapping(value = "/buffered", produces = MediaType.APPLICATION_JSON_VALUE)
    ResponseEntity<Object> buffered() {
      return new ResponseEntity<>("{\"data\":{\"value\":\"é汉😀\"}}", HttpStatus.OK);
    }

    @GetMapping(value = "/streamed", produces = MediaType.APPLICATION_JSON_VALUE)
    CompletableFuture<ResponseEntity<Object>> streamed() {
      return CompletableFuture.completedFuture(
          new ResponseEntity<>(
              new GraphQLResponseBody(
                  Map.of("data", Map.of("value", "é汉😀")),
                  bytes -> {},
                  success -> writeFinished[0] = success),
              HttpStatus.OK));
    }
  }
}
