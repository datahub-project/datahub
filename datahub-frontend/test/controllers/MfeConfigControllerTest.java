package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import java.io.File;
import java.nio.file.Files;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
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
    public void testConstructorThrowsIfPathNull() {
        when(mockConfig.getString("mfeConfigFilePath")).thenReturn(null);
        Exception ex = assertThrows(
                IllegalArgumentException.class,
                () -> new MfeConfigController(mockConfig, mockEnvironment)
        );
        assertTrue(ex.getMessage().contains("MfeConfigFilePath is null or not set!"));
    }

    @Test
    public void testGetMfeConfigReturnsInternalServerErrorIfPathNull() {
        // Bypass constructor check by reflection
        controller = mock(MfeConfigController.class);
        when(controller.getMfeConfig()).thenCallRealMethod();
        doReturn(null).when(controller).readFromYMLConfig(anyString());
        // Simulate configFilePath is null
        try {
            java.lang.reflect.Field field = MfeConfigController.class.getDeclaredField("configFilePath");
            field.setAccessible(true);
            field.set(controller, null);
        } catch (Exception e) {
            fail("Reflection setup failed: " + e.getMessage());
        }
        Result result = controller.getMfeConfig();
        assertEquals(500, result.status());
        assertTrue(result.body().toString().contains("Config File Path not set!"));
    }

    @Test
    public void testGetMfeConfigReturnsInternalServerErrorIfFileUnreadable() {
        when(mockConfig.getString("mfeConfigFilePath")).thenReturn("bad/path.yaml");
        controller = spy(new MfeConfigController(mockConfig, mockEnvironment));
        doReturn("").when(controller).readFromYMLConfig(anyString());
        Result result = controller.getMfeConfig();
        assertEquals(500, result.status());
        assertTrue(result.body().toString().contains("Unable to read config file"));
    }

    @Test
    public void testGetMfeConfigReturnsYamlContent() {
        when(mockConfig.getString("mfeConfigFilePath")).thenReturn("good/path.yaml");
        controller = spy(new MfeConfigController(mockConfig, mockEnvironment));
        doReturn("yaml: value").when(controller).readFromYMLConfig(anyString());
        Result result = controller.getMfeConfig();
        assertEquals(200, result.status());
        assertEquals("application/yaml", result.contentType().orElse(""));
        assertTrue(result.body().toString().contains("yaml: value"));
    }

    @Test
    public void testReadFromYMLConfigReadsFileSuccessfully() throws Exception {
        String fakePath = "test.yaml";
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

    @Test
    public void testReadFromYMLConfigReturnsEmptyOnException() {
        when(mockConfig.getString("mfeConfigFilePath")).thenReturn("missing.yaml");
        when(mockEnvironment.getFile("missing.yaml")).thenThrow(new RuntimeException("not found"));
        controller = new MfeConfigController(mockConfig, mockEnvironment);
        String result = controller.readFromYMLConfig("missing.yaml");
        assertEquals("", result);
    }
}