package mock;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.VersionedUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.schema.ForeignKeyConstraint;
import com.linkedin.schema.ForeignKeyConstraintArray;
import com.linkedin.schema.MySqlDDL;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaFieldArray;
import com.linkedin.schema.SchemaFieldDataType;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.schema.StringType;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static entities.EntitiesControllerTest.*;


public class MockEntityService extends EntityService {
  public MockEntityService(@Nonnull EventProducer producer, @Nonnull EntityRegistry entityRegistry) {
    super(producer, entityRegistry);
  }

  @Override
  public Map<Urn, List<RecordTemplate>> getLatestAspects(@Nonnull Set<Urn> urns, @Nonnull Set<String> aspectNames) {
    return null;
  }

  @Override
  public Map<String, RecordTemplate> getLatestAspectsForUrn(@Nonnull Urn urn, @Nonnull Set<String> aspectNames) {
    return Collections.emptyMap();
  }

  @Override
  public RecordTemplate getAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {
    return null;
  }

  @Override
  public Map<Urn, List<EnvelopedAspect>> getLatestEnvelopedAspects(@Nonnull String entityName, @Nonnull Set<Urn> urns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {
    Urn urn = UrnUtils.getUrn(DATASET_URN);
    Map<Urn, List<EnvelopedAspect>> envelopedAspectMap = new HashMap<>();
    List<EnvelopedAspect> aspects = new ArrayList<>();
    EnvelopedAspect schemaMetadata = new EnvelopedAspect();
    SchemaMetadata pegasusSchemaMetadata = new SchemaMetadata();
    pegasusSchemaMetadata.setDataset(DatasetUrn.createFromUrn(UrnUtils.getUrn(DATASET_URN)))
        .setVersion(0L)
        .setCreated(new AuditStamp().setActor(UrnUtils.getUrn(CORPUSER_URN)).setTime(System.currentTimeMillis()))
        .setHash(S)
        .setCluster(S)
        .setPlatformSchema(SchemaMetadata.PlatformSchema.create(new MySqlDDL().setTableSchema(S)))
        .setForeignKeys(new ForeignKeyConstraintArray(Collections.singletonList(
            new ForeignKeyConstraint()
                .setForeignDataset(urn)
                .setName(S)
                .setForeignFields(new UrnArray(Collections.singletonList(urn))))))
        .setFields(new SchemaFieldArray(Collections.singletonList(
            new SchemaField()
                .setDescription(S)
                .setFieldPath(S)
                .setType(new SchemaFieldDataType().setType(SchemaFieldDataType.Type.create(new StringType())))
                .setGlobalTags(
                    new GlobalTags()
                        .setTags(new TagAssociationArray(Collections.singletonList(
                            new TagAssociation().setTag(TagUrn.createFromUrn(UrnUtils.getUrn(TAG_URN)))
                        ))))
                .setGlossaryTerms(new GlossaryTerms().setTerms(
                    new GlossaryTermAssociationArray(Collections.singletonList(
                        new GlossaryTermAssociation()
                            .setUrn(GlossaryTermUrn.createFromUrn(UrnUtils.getUrn(GLOSSARY_TERM_URN)))
                    )))
                )
            ))
        );
    schemaMetadata
        .setType(AspectType.VERSIONED)
        .setName("schemaMetadata")
        .setValue(new Aspect(pegasusSchemaMetadata.data()));
    aspects.add(schemaMetadata);
    envelopedAspectMap.put(UrnUtils.getUrn(DATASET_URN), aspects);
    return envelopedAspectMap;
  }

  @Override
  public Map<Urn, List<EnvelopedAspect>> getVersionedEnvelopedAspects(@Nonnull Set<VersionedUrn> versionedUrns,
      @Nonnull Set<String> aspectNames) throws URISyntaxException {
    return null;
  }

  @Override
  public EnvelopedAspect getLatestEnvelopedAspect(@Nonnull String entityName, @Nonnull Urn urn,
      @Nonnull String aspectName) throws Exception {
    return null;
  }

  @Override
  public EnvelopedAspect getEnvelopedAspect(@Nonnull String entityName, @Nonnull Urn urn, @Nonnull String aspectName,
      long version) throws Exception {
    return null;
  }

  @Override
  public VersionedAspect getVersionedAspect(@Nonnull Urn urn, @Nonnull String aspectName, long version) {
    return null;
  }

  @Override
  public ListResult<RecordTemplate> listLatestAspects(@Nonnull String entityName, @Nonnull String aspectName, int start,
      int count) {
    return null;
  }

  @Nonnull
  @Override
  protected UpdateAspectResult ingestAspectToLocalDB(@Nonnull Urn urn, @Nonnull String aspectName,
      @Nonnull Function<Optional<RecordTemplate>, RecordTemplate> updateLambda, @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata systemMetadata) {
    return new UpdateAspectResult(UrnUtils.getUrn(DATASET_URN), null,
        null, null, null, null, null, 0L);
  }

  @Nonnull
  @Override
  protected List<Pair<String, UpdateAspectResult>> ingestAspectsToLocalDB(@Nonnull Urn urn,
      @Nonnull List<Pair<String, RecordTemplate>> aspectRecordsToIngest, @Nonnull AuditStamp auditStamp,
      @Nonnull SystemMetadata providedSystemMetadata) {
    return Collections.emptyList();
  }

  @Override
  public RecordTemplate updateAspect(@Nonnull Urn urn, @Nonnull String entityName, @Nonnull String aspectName,
      @Nonnull AspectSpec aspectSpec, @Nonnull RecordTemplate newValue, @Nonnull AuditStamp auditStamp,
      @Nonnull long version, @Nonnull boolean emitMae) {
    return null;
  }

  @Override
  public ListUrnsResult listUrns(@Nonnull String entityName, int start, int count) {
    return null;
  }

  @Override
  public void setWritable(boolean canWrite) {

  }

  @Override
  public RollbackRunResult rollbackWithConditions(List<AspectRowSummary> aspectRows, Map<String, String> conditions,
      boolean hardDelete) {
    return null;
  }

  @Override
  public RollbackRunResult deleteUrn(Urn urn) {
    return null;
  }

  @Override
  public Boolean exists(Urn urn) {
    return null;
  }
}
