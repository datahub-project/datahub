package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.Environment;
import play.mvc.Http;
import play.mvc.Result;
import play.test.Helpers;

public class MfeConfigControllerTest {

  private Config mockConfig;
  private Environment mockEnvironment;
  private MfeConfigController controller;

  @BeforeEach
  public void setUp() {
    mockConfig = mock(Config.class);
    mockEnvironment = mock(Environment.class);
  }

  @Test
  public void testGetMfeConfigWhenFilePathNull() {
    when(mockConfig.getString("mfeConfigFilePath")).thenReturn(null);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          new MfeConfigController(mockConfig, mockEnvironment);
        });
  }

  @Test
  public void testGetMfeConfigWhenFilePathNotNullButInvalid() {
    when(mockConfig.getString("mfeConfigFilePath")).thenReturn("bad/path.yaml");
    MfeConfigController mfeConfigController = new MfeConfigController(mockConfig, mockEnvironment);
    Result result = mfeConfigController.getMfeConfig();
    assertEquals(500, result.status());
  }

  @Test
  public void testGetMfeConfigReadsFileSuccessfully() throws Exception {
    String fileContent = "foo: bar";
    File tempFile = File.createTempFile("test", ".yaml");
    Files.writeString(tempFile.toPath(), fileContent);

    when(mockConfig.getString("mfeConfigFilePath")).thenReturn(tempFile.getName());
    when(mockEnvironment.getFile(tempFile.getName())).thenReturn(tempFile);

    controller = new MfeConfigController(mockConfig, mockEnvironment);

    Result result = controller.getMfeConfig();
    assertEquals(200, result.status());
    assertEquals("application/yaml", result.contentType().orElse(""));

    // Verify cache header is set
    assertTrue(result.header(Http.HeaderNames.CACHE_CONTROL).isPresent());
    assertTrue(result.header(Http.HeaderNames.CACHE_CONTROL).get().contains("max-age="));

    // Verify content matches
    String body = Helpers.contentAsString(result);
    assertEquals(fileContent, body);

    tempFile.delete();
  }

  @Test
  public void testGetMfeConfigReturnsCachedContent() throws Exception {
    String fileContent = "microFrontends: []";
    File tempFile = File.createTempFile("test", ".yaml");
    Files.writeString(tempFile.toPath(), fileContent);

    when(mockConfig.getString("mfeConfigFilePath")).thenReturn(tempFile.getName());
    when(mockEnvironment.getFile(tempFile.getName())).thenReturn(tempFile);

    controller = new MfeConfigController(mockConfig, mockEnvironment);

    // First call
    Result result1 = controller.getMfeConfig();
    assertEquals(200, result1.status());

    // Modify the file after controller initialization
    Files.writeString(tempFile.toPath(), "modified: content");

    // Second call should still return cached (original) content
    Result result2 = controller.getMfeConfig();
    assertEquals(200, result2.status());
    String body = Helpers.contentAsString(result2);
    assertEquals(fileContent, body);

    tempFile.delete();
  }
}
