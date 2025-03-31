package com.linkedin.datahub.graphql.types.common.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class QueryPropertiesMapperTest {

  private Urn _queryUrn;

  @BeforeMethod
  public void BeforeMethod() throws Exception {
    _queryUrn = Urn.createFromString("urn:li:query:1");
  }

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
        QueryPropertiesMapper.map(null, input, _queryUrn);

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
    StringMap customProps = new StringMap();
    customProps.put("key1", "value1");
    customProps.put("key2", "value2");
    input.setCustomProperties(customProps);

    // Map the object
    com.linkedin.datahub.graphql.generated.QueryProperties result =
        QueryPropertiesMapper.map(null, input, _queryUrn);

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
    assertNotNull(result.getCustomProperties());
    assertEquals(result.getCustomProperties().size(), 2);
    assertEquals(result.getCustomProperties().get(0).getKey(), "key1");
    assertEquals(result.getCustomProperties().get(0).getValue(), "value1");
    assertEquals(result.getCustomProperties().get(0).getAssociatedUrn(), _queryUrn.toString());
    assertEquals(result.getCustomProperties().get(1).getKey(), "key2");
    assertEquals(result.getCustomProperties().get(1).getValue(), "value2");
    assertEquals(result.getCustomProperties().get(1).getAssociatedUrn(), _queryUrn.toString());
    ;
  }
}
