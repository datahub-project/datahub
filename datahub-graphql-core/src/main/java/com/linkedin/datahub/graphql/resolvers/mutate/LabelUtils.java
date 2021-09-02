package com.linkedin.datahub.graphql.resolvers.mutate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.LabelUpdateInput;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.Snapshot;
import java.net.URISyntaxException;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.inject.Inject;
import javax.inject.Named;


public class LabelUtils {
  private LabelUtils() { }

  @Inject
  @Named("entityService")
  private static EntityService _entityService;

  public static Map<String, String> entityTypeToMethod = ImmutableMap.of(
      "tag", "getGlobalTags",
      "glossaryTerm", "getGlossaryTerms"
  );

  private Snapshot toSnapshotUnion(@Nonnull final RecordTemplate snapshotRecord) {
    final Snapshot snapshot = new Snapshot();
    RecordUtils.setSelectedRecordTemplateInUnion(snapshot, snapshotRecord);
    return snapshot;
  }

  public static Entity createNewEntityForUpdate(LabelUpdateInput input, Entity existingEntity)
      throws URISyntaxException {
    Urn labelUrn = Urn.createFromString(input.getLabelUrn());
    if (labelUrn.getEntityType() == "tag") {
      createNewEntityForTagUpdate(input, existingEntity);
    } else if (labelUrn.getEntityType() == "glossaryTerm") {
      createNewEntityForTermUpdate(input, existingEntity);
    } else {
      throw new RuntimeException(
          "Exception: trying to add type "
              + labelUrn.getEntityType()
              + " via add label endpoint. Types supported: tag, glossaryTerm"
      );
    }

    return _entityService.toSnapshotUnion(
        _entityService.toSnapshotRecord(
            input.getTargetUrn(),
            ImmutableList.of(toAspectUnion(urn, aspectValue))
        )
    );
  }

  private static void createNewEntityForTermUpdate(LabelUpdateInput input, Entity existingEntity) {
    if (input.getSubResource() == null || input.getSubResource().equals("")) {
      // add to top-level entity properties
    } else {
      // add to subresource
    }
  }

  private static void createNewEntityForTagUpdate(LabelUpdateInput input, Entity existingEntity) {
  }
}
