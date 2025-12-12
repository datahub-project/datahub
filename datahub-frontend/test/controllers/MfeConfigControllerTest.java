package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.Environment;
import play.mvc.Result;

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
  public void testgetMfeConfigWhenFilePathNotNullButInvalid() {
    when(mockConfig.getString("mfeConfigFilePath")).thenReturn("bad/path.yaml");
    MfeConfigController mfeConfigController = new MfeConfigController(mockConfig, mockEnvironment);
    Result result = mfeConfigController.getMfeConfig();
    assertEquals(500, result.status());
  }

  @Test
  public void testReadFromYMLConfigReadsFileSuccessfully() throws Exception {
    String fileContent = "foo: bar";
    File tempFile = File.createTempFile("test", ".yaml");
    Files.writeString(tempFile.toPath(), fileContent);

    when(mockConfig.getString("mfeConfigFilePath")).thenReturn(tempFile.getName());
    when(mockEnvironment.getFile(tempFile.getName())).thenReturn(tempFile);

    controller = new MfeConfigController(mockConfig, mockEnvironment);

    String result = controller.readFromYMLConfig(tempFile.getName());
    assertEquals(fileContent, result);

    tempFile.delete();
  }
}
