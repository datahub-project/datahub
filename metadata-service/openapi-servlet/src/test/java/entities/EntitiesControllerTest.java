package entities;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizerChain;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.config.PreProcessHooks;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.IngestAspectsResult;
import com.linkedin.metadata.entity.TransactionContext;
import com.linkedin.metadata.entity.TransactionResult;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.dto.UpsertAspectRequest;
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
import io.datahubproject.openapi.v1.entities.EntitiesController;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import io.ebean.Transaction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import mock.MockEntityService;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntitiesControllerTest {

  public static final String S = "somerandomstring";
  public static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:platform,name,PROD)";
  public static final String CORPUSER_URN = "urn:li:corpuser:datahub";
  public static final String GLOSSARY_TERM_URN = "urn:li:glossaryTerm:SavingAccount";
  public static final String DATA_PLATFORM_URN = "urn:li:dataPlatform:platform";
  public static final String TAG_URN = "urn:li:tag:sometag";

  @BeforeMethod
  public void setup() {

    OperationContext opContext = TestOperationContexts.systemContextNoSearchAuthorization();
    AspectDao aspectDao = Mockito.mock(AspectDao.class);
    when(aspectDao.runInTransactionWithRetry(
            ArgumentMatchers
                .<Function<TransactionContext, TransactionResult<List<UpdateAspectResult>>>>any(),
            any(AspectsBatch.class),
            anyInt()))
        .thenAnswer(
            i ->
                ((Function<TransactionContext, TransactionResult<IngestAspectsResult>>)
                        i.getArgument(0))
                    .apply(TransactionContext.empty(Mockito.mock(Transaction.class), 0))
                    .getResults());
    doReturn(Pair.of(Optional.empty(), Optional.empty()))
        .when(aspectDao)
        .saveLatestAspect(
            any(OperationContext.class),
            any(TransactionContext.class),
            nullable(SystemAspect.class),
            any(SystemAspect.class));

    EventProducer mockEntityEventProducer = Mockito.mock(EventProducer.class);
    PreProcessHooks preProcessHooks = new PreProcessHooks();
    preProcessHooks.setUiEnabled(true);
    MockEntityService mockEntityService =
        new MockEntityService(aspectDao, mockEntityEventProducer, preProcessHooks);
    AuthorizerChain authorizerChain = Mockito.mock(AuthorizerChain.class);
    _entitiesController =
        new EntitiesController(opContext, mockEntityService, new ObjectMapper(), authorizerChain);
    Authentication authentication = Mockito.mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "datahub"));
    when(authorizerChain.authorize(any()))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.ALLOW, ""));
    AuthenticationContext.setAuthentication(authentication);
  }

  EntitiesController _entitiesController;

  @Test
  public void testIngestDataset() {
    List<UpsertAspectRequest> datasetAspects = new ArrayList<>();
    UpsertAspectRequest viewProperties =
        UpsertAspectRequest.builder()
            .aspect(
                ViewProperties.builder().viewLogic(S).viewLanguage(S).materialized(true).build())
            .entityType(DATASET_ENTITY_NAME)
            .entityUrn(DATASET_URN)
            .build();
    datasetAspects.add(viewProperties);

    UpsertAspectRequest subTypes =
        UpsertAspectRequest.builder()
            .aspect(SubTypes.builder().typeNames(Collections.singletonList(S)).build())
            .entityType(DATASET_ENTITY_NAME)
            .entityKeyAspect(
                DatasetKey.builder()
                    .name("name")
                    .platform(DATA_PLATFORM_URN)
                    .origin(FabricType.PROD)
                    .build())
            .build();
    datasetAspects.add(subTypes);

    UpsertAspectRequest datasetProfile =
        UpsertAspectRequest.builder()
            .aspect(
                DatasetProfile.builder()
                    .build()
                    .timestampMillis(0L)
                    .addFieldProfilesItem(
                        DatasetFieldProfile.builder()
                            .fieldPath(S)
                            .histogram(
                                Histogram.builder()
                                    .boundaries(Collections.singletonList(S))
                                    .build())
                            .build()))
            .entityType(DATASET_ENTITY_NAME)
            .entityKeyAspect(
                DatasetKey.builder()
                    .name("name")
                    .platform(DATA_PLATFORM_URN)
                    .origin(FabricType.PROD)
                    .build())
            .build();
    datasetAspects.add(datasetProfile);

    UpsertAspectRequest schemaMetadata =
        UpsertAspectRequest.builder()
            .aspect(
                SchemaMetadata.builder()
                    .schemaName(S)
                    .dataset(DATASET_URN)
                    .platform(DATA_PLATFORM_URN)
                    .hash(S)
                    .version(0L)
                    .platformSchema(MySqlDDL.builder().tableSchema(S).build())
                    .fields(
                        Collections.singletonList(
                            SchemaField.builder()
                                .fieldPath(S)
                                .nativeDataType(S)
                                .type(
                                    SchemaFieldDataType.builder()
                                        .type(StringType.builder().build())
                                        .build())
                                .description(S)
                                .globalTags(
                                    GlobalTags.builder()
                                        .tags(
                                            Collections.singletonList(
                                                TagAssociation.builder().tag(TAG_URN).build()))
                                        .build())
                                .glossaryTerms(
                                    GlossaryTerms.builder()
                                        .terms(
                                            Collections.singletonList(
                                                GlossaryTermAssociation.builder()
                                                    .urn(GLOSSARY_TERM_URN)
                                                    .build()))
                                        .auditStamp(
                                            AuditStamp.builder()
                                                .time(0L)
                                                .actor(CORPUSER_URN)
                                                .build())
                                        .build())
                                .build()))
                    .build())
            .entityType(DATASET_ENTITY_NAME)
            .entityKeyAspect(
                DatasetKey.builder()
                    .name("name")
                    .platform(DATA_PLATFORM_URN)
                    .origin(FabricType.PROD)
                    .build())
            .build();
    datasetAspects.add(schemaMetadata);

    UpsertAspectRequest glossaryTerms =
        UpsertAspectRequest.builder()
            .aspect(
                GlossaryTerms.builder()
                    .terms(
                        Collections.singletonList(
                            GlossaryTermAssociation.builder().urn(GLOSSARY_TERM_URN).build()))
                    .auditStamp(AuditStamp.builder().time(0L).actor(CORPUSER_URN).build())
                    .build())
            .entityType(DATASET_ENTITY_NAME)
            .entityKeyAspect(
                DatasetKey.builder()
                    .name("name")
                    .platform(DATA_PLATFORM_URN)
                    .origin(FabricType.PROD)
                    .build())
            .build();
    datasetAspects.add(glossaryTerms);

    _entitiesController.postEntities(null, datasetAspects, false, false, false);
  }

  //  @Test
  //  public void testGetDataset() {
  //    _entitiesController.getEntities(new String[] {DATASET_URN},
  //        new String[] {
  //            SCHEMA_METADATA_ASPECT_NAME
  //    });
  //  }
}
