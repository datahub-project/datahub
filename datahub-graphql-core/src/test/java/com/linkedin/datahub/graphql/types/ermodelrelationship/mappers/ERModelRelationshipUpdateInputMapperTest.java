import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.ERModelRelationshipPropertiesInput;
import com.linkedin.datahub.graphql.generated.RelationshipFieldMappingInput;
import com.linkedin.datahub.graphql.types.ermodelrelationship.mappers.ERModelRelationshipUpdateInputMapper;
import com.linkedin.ermodelrelation.ERModelRelationshipCardinality;
import com.linkedin.ermodelrelation.RelationshipFieldMappingArray;
import java.lang.reflect.Method;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ERModelRelationshipUpdateInputMapperTest {

  private static final Urn TEST_ACTOR_URN = Urn.createFromTuple("corpuser", "testActor");

  @Test
  public void testCreateERModelRelationPropertiesWithFieldMappings() throws Exception {
    ERModelRelationshipPropertiesInput propertiesInput = new ERModelRelationshipPropertiesInput();
    propertiesInput.setName("Test Relationship");
    propertiesInput.setSource("urn:li:dataset:(testPlatform,testSource,PROD)");
    propertiesInput.setDestination("urn:li:dataset:(testPlatform,testDestination,PROD)");
    propertiesInput.setRelationshipFieldmappings(
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2")));

    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(TEST_ACTOR_URN);
    auditStamp.setTime(System.currentTimeMillis());

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();

    // Use reflection to access the private method
    Method method =
        ERModelRelationshipUpdateInputMapper.class.getDeclaredMethod(
            "createERModelRelationProperties",
            ERModelRelationshipPropertiesInput.class,
            AuditStamp.class);
    method.setAccessible(true);

    com.linkedin.ermodelrelation.ERModelRelationshipProperties properties =
        (com.linkedin.ermodelrelation.ERModelRelationshipProperties)
            method.invoke(mapper, propertiesInput, auditStamp);

    Assert.assertNotNull(properties.getRelationshipFieldMappings());
    Assert.assertEquals(properties.getRelationshipFieldMappings().size(), 2);
    Assert.assertEquals(
        properties.getRelationshipFieldMappings().get(0).getSourceField(), "sourceField1");
    Assert.assertEquals(
        properties.getRelationshipFieldMappings().get(0).getDestinationField(),
        "destinationField1");
  }

  @Test
  public void testCardinalitySettings() throws Exception {
    List<RelationshipFieldMappingInput> fieldMappings =
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2"));

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();

    // Use reflection to access the private method
    Method method =
        ERModelRelationshipUpdateInputMapper.class.getDeclaredMethod(
            "ermodelrelationCardinalitySettings", List.class, ERModelRelationshipCardinality.class);
    method.setAccessible(true);

    ERModelRelationshipCardinality cardinality =
        (ERModelRelationshipCardinality) method.invoke(mapper, fieldMappings, null);

    Assert.assertEquals(cardinality, ERModelRelationshipCardinality.ONE_ONE);
  }

  @Test
  public void testFieldMappingSettings() throws Exception {
    List<RelationshipFieldMappingInput> fieldMappings =
        List.of(
            new RelationshipFieldMappingInput("sourceField1", "destinationField1"),
            new RelationshipFieldMappingInput("sourceField2", "destinationField2"));

    ERModelRelationshipUpdateInputMapper mapper = new ERModelRelationshipUpdateInputMapper();

    // Use reflection to access the private method
    Method method =
        ERModelRelationshipUpdateInputMapper.class.getDeclaredMethod(
            "ermodelrelationFieldMappingSettings", List.class);
    method.setAccessible(true);

    RelationshipFieldMappingArray fieldMappingArray =
        (RelationshipFieldMappingArray) method.invoke(mapper, fieldMappings);

    Assert.assertNotNull(fieldMappingArray);
    Assert.assertEquals(fieldMappingArray.size(), 2);
    Assert.assertEquals(fieldMappingArray.get(0).getSourceField(), "sourceField1");
    Assert.assertEquals(fieldMappingArray.get(0).getDestinationField(), "destinationField1");
  }
}
