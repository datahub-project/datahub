package com.linkedin.metadata.boot.steps;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.patch.builder.StructuredPropertyDefinitionPatchBuilder;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class IngestStructuredPropertyExtensionsStep implements BootstrapStep {

  private EntityService<?> entityService;
  private Map<String, StructuredPropertyDefinition> structuredPropertyMappings;

  public IngestStructuredPropertyExtensionsStep(
      EntityService<?> entityService,
      Map<String, StructuredPropertyDefinition> structuredPropertyMappings) {
    this.entityService = entityService;
    this.structuredPropertyMappings = structuredPropertyMappings;
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void execute(@NotNull OperationContext opContext) throws Exception {
    // Bootstrap structured properties definitions if enabled
    if (!structuredPropertyMappings.isEmpty()) {
      // May result in extraneous writes from multiple GMS, but backend will handle
      List<MetadataChangeProposal> proposals = new ArrayList<>();
      structuredPropertyMappings
          .values()
          .forEach(
              structuredPropertyDefinition -> {
                StructuredPropertyDefinitionPatchBuilder builder =
                    new StructuredPropertyDefinitionPatchBuilder()
                        .urn(
                            StructuredPropertyUtils.toURNFromFQN(
                                structuredPropertyDefinition.getQualifiedName()));

                builder.setQualifiedName(structuredPropertyDefinition.getQualifiedName());
                builder.setValueType(structuredPropertyDefinition.getValueType().toString());
                structuredPropertyDefinition.getEntityTypes().stream()
                    .map(Urn::toString)
                    .forEach(builder::addEntityType);
                if (structuredPropertyDefinition.getDisplayName() != null) {
                  builder.setDisplayName(structuredPropertyDefinition.getDisplayName());
                }
                if (structuredPropertyDefinition.getDescription() != null) {
                  builder.setDescription(structuredPropertyDefinition.getDescription());
                }

                builder.setImmutable(structuredPropertyDefinition.isImmutable());

                if (structuredPropertyDefinition.getTypeQualifier() != null) {
                  builder.setTypeQualifier(structuredPropertyDefinition.getTypeQualifier());
                }
                if (structuredPropertyDefinition.getAllowedValues() != null) {
                  structuredPropertyDefinition.getAllowedValues().forEach(builder::addAllowedValue);
                }
                if (structuredPropertyDefinition.getCardinality() != null) {
                  builder.setCardinality(structuredPropertyDefinition.getCardinality());
                }

                MetadataChangeProposal mcp = builder.build();
                proposals.add(mcp);
                log.info("Adding structured property: {}", mcp.getEntityUrn());
              });
      AspectsBatch aspectsBatch =
          AspectsBatchImpl.builder()
              .mcps(proposals, opContext.getAuditStamp(), opContext.getRetrieverContext())
              .build(opContext);
      entityService.ingestProposal(opContext, aspectsBatch, false);
    }
  }

  @NotNull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }
}
