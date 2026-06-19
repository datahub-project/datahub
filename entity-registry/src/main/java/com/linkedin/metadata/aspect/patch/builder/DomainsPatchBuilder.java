package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DomainsPatchBuilder extends AbstractMultiFieldPatchBuilder<DomainsPatchBuilder> {

  private static final String BASE_PATH = "/domainAssociations/";
  private static final String DOMAIN_KEY = "domain";
  private static final String CONTEXT_KEY = "context";
  private static final String ATTRIBUTION_SOURCE_KEY = "attribution\u241fsource";

  private final List<ImmutableTriple<String, String, JsonNode>> wildcardRemoveOps =
      new ArrayList<>();

  private static final Map<String, List<String>> WILDCARD_REMOVE_APK =
      Collections.singletonMap(
          "domainAssociations",
          Collections.unmodifiableList(Arrays.asList(DOMAIN_KEY, ATTRIBUTION_SOURCE_KEY)));

  public DomainsPatchBuilder setDomain(@Nonnull Urn urn) {
    // remove existing list of domains to set the new one
    this.pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH, null));
    // add empty domains object node
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH, instance.objectNode()));
    // set the new domain
    addDomain(urn, null);
    return this;
  }

  public DomainsPatchBuilder addDomain(@Nonnull Urn domainUrn, @Nullable String context) {
    ObjectNode value = instance.objectNode();
    value.put(DOMAIN_KEY, domainUrn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(), BASE_PATH + "/" + encodeValueUrn(domainUrn), value));
    return this;
  }

  public DomainsPatchBuilder addDomain(
      @Nonnull Urn domainUrn, @Nullable String context, @Nonnull Urn attributionSource) {
    ObjectNode value = instance.objectNode();
    value.put(DOMAIN_KEY, domainUrn.toString());

    if (context != null) {
      value.put(CONTEXT_KEY, context);
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + encodeValue(attributionSource.toString()) + "/" + encodeValueUrn(domainUrn),
            value));
    return this;
  }

  public DomainsPatchBuilder removeDomain(@Nonnull Urn domainUrn) {
    wildcardRemoveOps.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(), BASE_PATH + encodeValueUrn(domainUrn), null));
    return this;
  }

  public DomainsPatchBuilder removeDomain(@Nonnull Urn domainUrn, @Nonnull Urn attributionSource) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            BASE_PATH + encodeValue(attributionSource.toString()) + "/" + encodeValueUrn(domainUrn),
            null));
    return this;
  }

  @Override
  public MetadataChangeProposal build() {
    if (!pathValues.isEmpty() && !wildcardRemoveOps.isEmpty()) {
      throw new IllegalStateException(
          "Cannot build a single patch for mixed add/attributed-remove and unattributed-remove "
              + "operations — each requires a different arrayPrimaryKeys ordering. "
              + "Use buildAll() to emit separate MCPs.");
    }
    if (pathValues.isEmpty() && !wildcardRemoveOps.isEmpty()) {
      return buildMcpForOps(wildcardRemoveOps, WILDCARD_REMOVE_APK);
    }
    return super.build();
  }

  @Override
  public List<MetadataChangeProposal> buildAll() {
    if (pathValues.isEmpty() && wildcardRemoveOps.isEmpty()) {
      throw new IllegalArgumentException("No patches specified.");
    }
    List<MetadataChangeProposal> result = new ArrayList<>();
    if (!pathValues.isEmpty()) {
      result.add(super.build());
    }
    if (!wildcardRemoveOps.isEmpty()) {
      result.add(buildMcpForOps(wildcardRemoveOps, WILDCARD_REMOVE_APK));
    }
    return result;
  }

  private MetadataChangeProposal buildMcpForOps(
      List<ImmutableTriple<String, String, JsonNode>> ops,
      @Nullable Map<String, List<String>> apk) {
    ArrayNode patches = instance.arrayNode();
    ops.forEach(
        triple ->
            patches.add(
                instance
                    .objectNode()
                    .put(OP_KEY, triple.left)
                    .put(PATH_KEY, triple.middle)
                    .set(VALUE_KEY, triple.right)));

    JsonNode payload;
    if (apk != null) {
      ObjectNode apkNode = instance.objectNode();
      for (Map.Entry<String, List<String>> entry : apk.entrySet()) {
        ArrayNode keys = instance.arrayNode();
        entry.getValue().forEach(keys::add);
        apkNode.set(entry.getKey(), keys);
      }
      ObjectNode envelope = instance.objectNode();
      envelope.set("arrayPrimaryKeys", apkNode);
      envelope.set("patch", patches);
      payload = envelope;
    } else {
      payload = patches;
    }

    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setContentType("application/json-patch+json");
    genericAspect.setValue(ByteString.copyString(payload.toString(), StandardCharsets.UTF_8));

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setChangeType(ChangeType.PATCH);
    proposal.setEntityType(getEntityType());
    proposal.setEntityUrn(this.targetEntityUrn);
    proposal.setAspectName(getAspectName());
    proposal.setAspect(genericAspect);
    return proposal;
  }

  @Override
  protected Map<String, List<String>> getArrayPrimaryKeys() {
    return Collections.singletonMap(
        "domainAssociations",
        Collections.unmodifiableList(Arrays.asList(ATTRIBUTION_SOURCE_KEY, DOMAIN_KEY)));
  }

  @Override
  protected String getAspectName() {
    return DOMAINS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
