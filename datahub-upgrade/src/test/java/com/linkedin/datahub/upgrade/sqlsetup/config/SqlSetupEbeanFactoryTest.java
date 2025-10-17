package com.linkedin.datahub.upgrade.sqlsetup.config;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import io.ebean.Database;
import io.ebean.DatabaseFactory;
import io.ebean.config.DatabaseConfig;
import io.ebean.datasource.DataSourceConfig;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SqlSetupEbeanFactoryTest {

  @Mock private DatabaseConfig mockDatabaseConfig;
  @Mock private DataSourceConfig mockDataSourceConfig;

  private SqlSetupEbeanFactory sqlSetupEbeanFactory;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    sqlSetupEbeanFactory = new SqlSetupEbeanFactory();

    // Set up environment variables for testing
    System.setProperty("CREATE_DB", "true");
  }

  @Test
  public void testCreateServerWithMysqlAndCreateDb() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method
    Database mockDatabase = mock(Database.class);
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic.when(() -> DatabaseFactory.create(mockDatabaseConfig)).thenReturn(mockDatabase);

      Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

      assertNotNull(result);
      assertEquals(result, mockDatabase);

      // Verify that the URL was modified to remove database name
      verify(mockDataSourceConfig).url(contains("jdbc:mysql://localhost:3306/"));
    }
  }

  @Test
  public void testCreateServerWithMysqlAndNoCreateDb() {
    System.setProperty("CREATE_DB", "false");

    String originalUrl = "jdbc:mysql://localhost:3306/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method
    Database mockDatabase = mock(Database.class);
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic.when(() -> DatabaseFactory.create(mockDatabaseConfig)).thenReturn(mockDatabase);

      Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

      assertNotNull(result);
      assertEquals(result, mockDatabase);

      // Verify that the original URL was kept
      verify(mockDataSourceConfig).url(originalUrl);
    }
  }

  @Test
  public void testCreateServerWithPostgres() {
    String originalUrl = "jdbc:postgresql://localhost:5432/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method
    Database mockDatabase = mock(Database.class);
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic.when(() -> DatabaseFactory.create(mockDatabaseConfig)).thenReturn(mockDatabase);

      Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

      assertNotNull(result);
      assertEquals(result, mockDatabase);

      // Verify that the original URL was kept for PostgreSQL
      verify(mockDataSourceConfig).url(originalUrl);
    }
  }

  @Test
  public void testCreateServerWithException() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method to throw exception
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic
          .when(() -> DatabaseFactory.create(mockDatabaseConfig))
          .thenThrow(new RuntimeException("Database connection failed"));

      try {
        sqlSetupEbeanFactory.createServer(mockDatabaseConfig);
        assertTrue(false, "Expected RuntimeException to be thrown");
      } catch (RuntimeException e) {
        assertEquals(e.getMessage(), "Database connection failed");
      }
    }
  }

  @Test
  public void testCreateServerWithDifferentUrls() {
    // Test various URL formats
    String[] testUrls = {
      "jdbc:mysql://localhost:3306/datahub",
      "jdbc:mysql://localhost/datahub",
      "jdbc:postgresql://localhost:5432/datahub",
      "jdbc:postgresql://localhost/datahub",
      "jdbc:mysql://user:pass@localhost:3306/datahub",
      "jdbc:postgresql://user:pass@localhost:5432/datahub"
    };

    for (String testUrl : testUrls) {
      when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
      when(mockDataSourceConfig.getUrl()).thenReturn(testUrl);

      // Mock the DatabaseFactory.create method
      Database mockDatabase = mock(Database.class);
      try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
        mockedStatic
            .when(() -> DatabaseFactory.create(mockDatabaseConfig))
            .thenReturn(mockDatabase);

        Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

        assertNotNull(result);
        assertEquals(result, mockDatabase);
      }
    }
  }

  @Test
  public void testCreateServerWithVariousUrlFormats() {
    String[] testUrls = {
      "jdbc:mysql://localhost:3306/datahub",
      "jdbc:mysql://localhost/datahub",
      "jdbc:mysql://user:pass@localhost:3306/datahub",
      "jdbc:mysql://user:pass@localhost/datahub",
      "jdbc:postgresql://localhost:5432/datahub",
      "jdbc:postgresql://localhost/datahub",
      "jdbc:postgresql://user:pass@localhost:5432/datahub",
      "jdbc:postgresql://user:pass@localhost/datahub",
      "jdbc:mysql://localhost:3306/datahub?useSSL=false",
      "jdbc:postgresql://localhost:5432/datahub?sslmode=require"
    };

    for (String testUrl : testUrls) {
      when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
      when(mockDataSourceConfig.getUrl()).thenReturn(testUrl);

      // Mock the DatabaseFactory.create method
      Database mockDatabase = mock(Database.class);
      try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
        mockedStatic
            .when(() -> DatabaseFactory.create(mockDatabaseConfig))
            .thenReturn(mockDatabase);

        Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

        assertNotNull(result);
        assertEquals(result, mockDatabase);
      }
    }
  }

  @Test
  public void testCreateServerWithCreateDbFalse() {
    System.setProperty("CREATE_DB", "false");

    String nonDBUrl = "jdbc:mysql://localhost:3306/";
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method
    Database mockDatabase = mock(Database.class);
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic.when(() -> DatabaseFactory.create(mockDatabaseConfig)).thenReturn(mockDatabase);

      Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

      assertNotNull(result);
      assertEquals(result, mockDatabase);

      // Verify that the original URL was kept
      verify(mockDataSourceConfig).url(originalUrl);
    }
  }

  @Test
  public void testCreateServerWithPostgresAlwaysKeepsOriginalUrl() {
    String originalUrl = "jdbc:postgresql://localhost:5432/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method
    Database mockDatabase = mock(Database.class);
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic.when(() -> DatabaseFactory.create(mockDatabaseConfig)).thenReturn(mockDatabase);

      Database result = sqlSetupEbeanFactory.createServer(mockDatabaseConfig);

      assertNotNull(result);
      assertEquals(result, mockDatabase);

      // Verify that the original URL was kept for PostgreSQL
      verify(mockDataSourceConfig).url(originalUrl);
    }
  }

  @Test
  public void testCreateServerWithExceptionHandling() {
    String originalUrl = "jdbc:mysql://localhost:3306/datahub";
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn(originalUrl);

    // Mock the DatabaseFactory.create method to throw exception
    try (var mockedStatic = mockStatic(DatabaseFactory.class)) {
      mockedStatic
          .when(() -> DatabaseFactory.create(mockDatabaseConfig))
          .thenThrow(new RuntimeException("Database connection failed"));

      try {
        sqlSetupEbeanFactory.createServer(mockDatabaseConfig);
        assertTrue(false, "Expected RuntimeException to be thrown");
      } catch (RuntimeException e) {
        assertEquals(e.getMessage(), "Database connection failed");
      }
    }
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateServerWithEmptyUrl() {
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(mockDataSourceConfig);
    when(mockDataSourceConfig.getUrl()).thenReturn("");

    // This should throw an IllegalArgumentException because empty URLs are not valid
    sqlSetupEbeanFactory.createServer(mockDatabaseConfig);
  }

  @Test
  public void testCreateServerWithNullDataSourceConfig() {
    when(mockDatabaseConfig.getDataSourceConfig()).thenReturn(null);

    try {
      sqlSetupEbeanFactory.createServer(mockDatabaseConfig);
      assertTrue(false, "Expected NullPointerException to be thrown");
    } catch (NullPointerException e) {
      // Expected behavior
    }
  }
}
