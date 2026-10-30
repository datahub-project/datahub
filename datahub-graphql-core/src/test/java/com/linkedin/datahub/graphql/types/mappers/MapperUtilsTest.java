package com.linkedin.datahub.graphql.types.mappers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.schema.annotation.PathSpecBasedSchemaAnnotationVisitor;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
import com.linkedin.datahub.graphql.generated.MatchedField;
import com.linkedin.datahub.graphql.generated.ResolvedAuditStamp;
import com.linkedin.metadata.entity.validation.ValidationApiUtils;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import java.util.List;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class MapperUtilsTest {
  private EntityRegistry entityRegistry;

  @BeforeTest
  public void setup() {
    PathSpecBasedSchemaAnnotationVisitor.class
        .getClassLoader()
        .setClassAssertionStatus(PathSpecBasedSchemaAnnotationVisitor.class.getName(), false);
    entityRegistry =
        new ConfigEntityRegistry(
            Snapshot.class.getClassLoader().getResourceAsStream("entity-registry.yml"));
  }

  @Test
  public void testMatchedFieldValidation() throws URISyntaxException {
    final Urn urn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:s3,urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29,PROD)");
    final Urn invalidUrn =
        Urn.createFromString(
            "urn:li:dataset:%28urn:li:dataPlatform:s3%2Ctest-datalake-concepts/prog_maintenance%2CPROD%29");
    assertThrows(
        IllegalArgumentException.class,
        () -> ValidationApiUtils.validateUrn(entityRegistry, invalidUrn));

    QueryContext mockContext = TestUtils.getMockAllowContext();

    List<MatchedField> actualMatched =
        MapperUtils.getMatchedFieldEntry(
            mockContext,
            List.of(
                buildSearchMatchField(urn.toString()),
                buildSearchMatchField(invalidUrn.toString())));

    assertEquals(actualMatched.size(), 2, "Matched fields should be 2");
    assertEquals(
        actualMatched.stream().filter(matchedField -> matchedField.getEntity() != null).count(),
        1,
        "With urn should be 1");
  }

  @Test
  public void testCreateResolvedAuditStamp_CorpUser() throws Exception {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(Urn.createFromString("urn:li:corpuser:datahub"));
    stamp.setTime(1234567890L);

    ResolvedAuditStamp result = MapperUtils.createResolvedAuditStamp(stamp);

    assertNotNull(result.getActor());
    assertEquals(result.getActor().getUrn(), "urn:li:corpuser:datahub");
    assertEquals(result.getTime(), 1234567890L);
  }

  @Test
  public void testCreateResolvedAuditStamp_NonCorpUser() throws Exception {
    AuditStamp stamp = new AuditStamp();
    stamp.setActor(Urn.createFromString("urn:li:aiAgent:my-agent"));
    stamp.setTime(1234567890L);

    ResolvedAuditStamp result = MapperUtils.createResolvedAuditStamp(stamp);

    assertNull(result.getActor());
    assertEquals(result.getTime(), 1234567890L);
  }

  private static com.linkedin.metadata.search.MatchedField buildSearchMatchField(
      String highlightValue) {
    com.linkedin.metadata.search.MatchedField field =
        new com.linkedin.metadata.search.MatchedField();
    field.setName("testField");
    field.setValue(highlightValue);
    return field;
  }
}
