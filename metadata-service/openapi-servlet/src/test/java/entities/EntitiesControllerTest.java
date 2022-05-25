package entities;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
import io.datahubproject.openapi.entities.EntitiesController;
import io.datahubproject.openapi.generated.AuditStamp;
import io.datahubproject.openapi.generated.DatasetFieldProfile;
import io.datahubproject.openapi.generated.DatasetKey;
import io.datahubproject.openapi.generated.DatasetProfile;
import io.datahubproject.openapi.generated.FabricType;
import io.datahubproject.openapi.generated.GlobalTags;
import io.datahubproject.openapi.generated.GlossaryTermAssociation;
import io.datahubproject.openapi.generated.GlossaryTerms;
import io.datahubproject.openapi.generated.Histogram;
import io.datahubproject.openapi.generated.MySqlDDL;
import io.datahubproject.openapi.generated.SchemaField;
import io.datahubproject.openapi.generated.SchemaFieldDataType;
import io.datahubproject.openapi.generated.SchemaMetadata;
import io.datahubproject.openapi.generated.StringType;
import io.datahubproject.openapi.generated.SubTypes;
import io.datahubproject.openapi.generated.TagAssociation;
import io.datahubproject.openapi.generated.ViewProperties;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import mock.MockEntityRegistry;
import mock.MockEntityService;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;


public class EntitiesControllerTest {

  public static final String S = "somerandomstring";
  public static final String DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:platform,name,PROD)";
  public static final String CORPUSER_URN = "urn:li:corpuser:datahub";
  public static final String GLOSSARY_TERM_URN = "urn:li:glossaryTerm:SavingAccount";
  public static final String DATA_PLATFORM_URN = "urn:li:dataPlatform:platform";
  public static final String TAG_URN = "urn:li:tag:sometag";

  @BeforeMethod
  public void setup()
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    EntityRegistry mockEntityRegistry = new MockEntityRegistry();
    AspectDao aspectDao = Mockito.mock(AspectDao.class);
    EventProducer mockEntityEventProducer = Mockito.mock(EventProducer.class);
    MockEntityService mockEntityService = new MockEntityService(aspectDao, mockEntityEventProducer, mockEntityRegistry);
    _entitiesController = new EntitiesController(mockEntityService, new ObjectMapper());
  }

  EntitiesController _entitiesController;

  @Test
  public void testIngestDataset() {
    List<UpsertAspectRequest> datasetAspects = new ArrayList<>();
    UpsertAspectRequest viewProperties = UpsertAspectRequest.builder()
        .aspect(new ViewProperties()
            .viewLogic(S)
            .viewLanguage(S)
            .materialized(true))
        .entityType(DATASET_ENTITY_NAME)
        .entityUrn(DATASET_URN)
        .build();
    datasetAspects.add(viewProperties);

    UpsertAspectRequest subTypes = UpsertAspectRequest.builder()
        .aspect(new SubTypes()
            .typeNames(Collections.singletonList(S)))
        .entityType(DATASET_ENTITY_NAME)
        .entityKeyAspect(new DatasetKey()
            .name("name")
            .platform(DATA_PLATFORM_URN)
            .origin(FabricType.PROD))
        .build();
    datasetAspects.add(subTypes);

    UpsertAspectRequest datasetProfile = UpsertAspectRequest.builder()
        .aspect(new DatasetProfile().timestampMillis(0L).addFieldProfilesItem(
            new DatasetFieldProfile()
                .fieldPath(S)
                .histogram(new Histogram()
                    .boundaries(Collections.singletonList(S))))
        )
        .entityType(DATASET_ENTITY_NAME)
        .entityKeyAspect(new DatasetKey()
            .name("name")
            .platform(DATA_PLATFORM_URN)
            .origin(FabricType.PROD))
        .build();
    datasetAspects.add(datasetProfile);

    UpsertAspectRequest schemaMetadata = UpsertAspectRequest.builder()
        .aspect(new SchemaMetadata()
            .schemaName(S)
            .dataset(DATASET_URN)
            .platform(DATA_PLATFORM_URN)
            .hash(S)
            .version(0L)
            .platformSchema(new MySqlDDL().tableSchema(S))
            .fields(Collections.singletonList(new SchemaField()
                .fieldPath(S)
                .nativeDataType(S)
                .type(new SchemaFieldDataType().type(new StringType()))
                .description(S)
                .globalTags(new GlobalTags()
                    .tags(Collections.singletonList(new TagAssociation()
                        .tag(TAG_URN))))
                .glossaryTerms(new GlossaryTerms()
                    .terms(Collections.singletonList(new GlossaryTermAssociation()
                        .urn(GLOSSARY_TERM_URN)))
                    .auditStamp(new AuditStamp()
                        .time(0L)
                        .actor(CORPUSER_URN)))
            )
        ))
        .entityType(DATASET_ENTITY_NAME)
        .entityKeyAspect(new DatasetKey()
            .name("name")
            .platform(DATA_PLATFORM_URN)
            .origin(FabricType.PROD))
        .build();
    datasetAspects.add(schemaMetadata);

    UpsertAspectRequest glossaryTerms = UpsertAspectRequest.builder()
        .aspect(new GlossaryTerms()
            .terms(Collections.singletonList(new GlossaryTermAssociation()
                .urn(GLOSSARY_TERM_URN)))
            .auditStamp(new AuditStamp()
                .time(0L)
                .actor(CORPUSER_URN)))
        .entityType(DATASET_ENTITY_NAME)
        .entityKeyAspect(new DatasetKey()
            .name("name")
            .platform(DATA_PLATFORM_URN)
            .origin(FabricType.PROD))
        .build();
    datasetAspects.add(glossaryTerms);

    _entitiesController.postEntities(datasetAspects);
  }

//  @Test
//  public void testGetDataset() {
//    _entitiesController.getEntities(new String[] {DATASET_URN},
//        new String[] {
//            SCHEMA_METADATA_ASPECT_NAME
//    });
//  }
}
