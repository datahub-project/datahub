package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import org.testng.annotations.Test;

public class QueryPropertiesMapperTest {

  @Test
  public void testMapWithRequiredFields() throws Exception {
    // Create test data
    QueryProperties input = new QueryProperties();

    // Set required fields
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM table");
    statement.setLanguage(QueryLanguage.SQL);
    input.setStatement(statement);

    input.setSource(QuerySource.MANUAL);

    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");

    AuditStamp created = new AuditStamp();
    created.setTime(1000L);
    created.setActor(userUrn);
    input.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(2000L);
    lastModified.setActor(userUrn);
    input.setLastModified(lastModified);

    // Map the object
    com.linkedin.datahub.graphql.generated.QueryProperties result =
        QueryPropertiesMapper.map(null, input);

    // Verify required fields
    assertNotNull(result);
    assertEquals(result.getSource().toString(), "MANUAL");
    assertEquals(result.getStatement().getValue(), "SELECT * FROM table");
    assertEquals(result.getStatement().getLanguage().toString(), "SQL");

    // Verify audit stamps
    assertEquals(result.getCreated().getTime().longValue(), 1000L);
    assertEquals(result.getCreated().getActor(), userUrn.toString());
    assertEquals(result.getLastModified().getTime().longValue(), 2000L);
    assertEquals(result.getLastModified().getActor(), userUrn.toString());

    // Verify optional fields are null
    assertNull(result.getName());
    assertNull(result.getDescription());
    assertNull(result.getOrigin());
  }

  @Test
  public void testMapWithOptionalFields() throws Exception {
    // Create test data
    QueryProperties input = new QueryProperties();

    // Set required fields
    QueryStatement statement = new QueryStatement();
    statement.setValue("SELECT * FROM table");
    statement.setLanguage(QueryLanguage.SQL);
    input.setStatement(statement);

    input.setSource(QuerySource.SYSTEM);

    Urn userUrn = Urn.createFromString("urn:li:corpuser:test");
    Urn originUrn = Urn.createFromString("urn:li:dataset:test");

    AuditStamp created = new AuditStamp();
    created.setTime(1000L);
    created.setActor(userUrn);
    input.setCreated(created);

    AuditStamp lastModified = new AuditStamp();
    lastModified.setTime(2000L);
    lastModified.setActor(userUrn);
    input.setLastModified(lastModified);

    // Set optional fields
    input.setName("Test Query");
    input.setDescription("Test Description");
    input.setOrigin(originUrn);

    // Map the object
    com.linkedin.datahub.graphql.generated.QueryProperties result =
        QueryPropertiesMapper.map(null, input);

    // Verify required fields
    assertNotNull(result);
    assertEquals(result.getSource().toString(), "SYSTEM");
    assertEquals(result.getStatement().getValue(), "SELECT * FROM table");
    assertEquals(result.getStatement().getLanguage().toString(), "SQL");

    // Verify audit stamps
    assertEquals(result.getCreated().getTime().longValue(), 1000L);
    assertEquals(result.getCreated().getActor(), userUrn.toString());
    assertEquals(result.getLastModified().getTime().longValue(), 2000L);
    assertEquals(result.getLastModified().getActor(), userUrn.toString());

    // Verify optional fields
    assertEquals(result.getName(), "Test Query");
    assertEquals(result.getDescription(), "Test Description");
    assertNotNull(result.getOrigin());
    assertEquals(result.getOrigin().getUrn(), originUrn.toString());
  }
}
