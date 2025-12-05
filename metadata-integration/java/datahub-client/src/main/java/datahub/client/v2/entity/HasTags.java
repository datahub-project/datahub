package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.common.TagAssociationArray;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.patch.builder.GlobalTagsPatchBuilder;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.v2.operations.EntityClient;
import datahub.event.MetadataChangeProposalWrapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mixin interface providing tag operations for entities that support tags.
 *
 * <p>Entities implementing this interface can have tags assigned for classification and discovery.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.addTag("pii");
 * dataset.addTag("urn:li:tag:sensitive");
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasTags<T extends Entity & HasTags<T>> {

  Logger log = LoggerFactory.getLogger(HasTags.class);
  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Adds a tag to this entity.
   *
   * @param tagUrn the URN of the tag (can be just the tag name, will be converted to URN)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addTag(@Nonnull String tagUrn) {
    String fullTagUrn = tagUrn.startsWith("urn:li:tag:") ? tagUrn : "urn:li:tag:" + tagUrn;
    try {
      return addTag(TagUrn.createFromString(fullTagUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(fullTagUrn, e);
    }
  }

  /**
   * Adds a tag to this entity.
   *
   * @param tagUrn the tag URN object
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addTag(@Nonnull TagUrn tagUrn) {
    Entity entity = (Entity) this;

    // Get or create accumulated patch builder for globalTags
    GlobalTagsPatchBuilder builder =
        entity.getPatchBuilder("globalTags", GlobalTagsPatchBuilder.class);
    if (builder == null) {
      builder = new GlobalTagsPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("globalTags", builder);
    }

    builder.addTag(tagUrn, null);
    return (T) this;
  }

  /**
   * Removes a tag from this entity.
   *
   * @param tagUrn the URN of the tag to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeTag(@Nonnull String tagUrn) {
    String fullTagUrn = tagUrn.startsWith("urn:li:tag:") ? tagUrn : "urn:li:tag:" + tagUrn;
    try {
      return removeTag(TagUrn.createFromString(fullTagUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(fullTagUrn, e);
    }
  }

  /**
   * Removes a tag from this entity.
   *
   * @param tagUrn the tag URN object to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeTag(@Nonnull TagUrn tagUrn) {
    Entity entity = (Entity) this;

    // Get or create accumulated patch builder for globalTags
    GlobalTagsPatchBuilder builder =
        entity.getPatchBuilder("globalTags", GlobalTagsPatchBuilder.class);
    if (builder == null) {
      builder = new GlobalTagsPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("globalTags", builder);
    }

    builder.removeTag(tagUrn);
    return (T) this;
  }

  /**
   * Sets the complete list of tags for this entity, replacing any existing tags.
   *
   * <p>Unlike {@link #addTag(String)} which creates a patch, this method creates a full aspect
   * replacement. This ensures that ONLY the specified tags will be present after save.
   *
   * @param tagUrns the list of tag URNs to set (can be just tag names, will be converted to URNs)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setTags(@Nonnull List<String> tagUrns) {
    Entity entity = (Entity) this;

    // Convert tag strings to TagUrn objects
    List<TagUrn> tags =
        tagUrns.stream()
            .map(
                tagUrn -> {
                  String fullTagUrn =
                      tagUrn.startsWith("urn:li:tag:") ? tagUrn : "urn:li:tag:" + tagUrn;
                  try {
                    return TagUrn.createFromString(fullTagUrn);
                  } catch (URISyntaxException e) {
                    throw new datahub.client.v2.exceptions.InvalidUrnException(fullTagUrn, e);
                  }
                })
            .collect(Collectors.toList());

    // Create full GlobalTags aspect with complete list
    GlobalTags globalTags = new GlobalTags();
    TagAssociationArray tagArray = new TagAssociationArray();

    for (TagUrn tagUrn : tags) {
      TagAssociation tagAssoc = new TagAssociation();
      tagAssoc.setTag(tagUrn);
      tagArray.add(tagAssoc);
    }

    globalTags.setTags(tagArray);

    // Create MCP wrapper for full aspect replacement
    MetadataChangeProposalWrapper mcp =
        MetadataChangeProposalWrapper.builder()
            .entityType(entity.getEntityType())
            .entityUrn(entity.getUrn())
            .upsert()
            .aspect(globalTags)
            .build();

    // Add to pending MCPs (this clears any patches for globalTags)
    entity.addPendingMCP(mcp);

    return (T) this;
  }

  /**
   * Gets the list of tags associated with this entity.
   *
   * @return the list of tag associations, or null if no tags are present
   */
  default List<TagAssociation> getTags() {
    Entity entity = (Entity) this;
    GlobalTags aspect = entity.getAspectLazy(GlobalTags.class);
    return aspect != null && aspect.hasTags() ? aspect.getTags() : null;
  }

  /**
   * Transforms a tag patch MCP to a full aspect replacement MCP via read-modify-write.
   *
   * <p>This is used by the VersionAwarePatchTransformer to work around broken tag patches on older
   * DataHub servers (&lt;= v1.3.0).
   *
   * <p>Steps:
   *
   * <ol>
   *   <li>Fetch current globalTags aspect from server
   *   <li>Apply patch operations to the fetched aspect locally
   *   <li>Return full aspect MCP with UPSERT change type
   * </ol>
   *
   * @param patch the tag patch MCP to transform
   * @param client the EntityClient for fetching current aspect
   * @return full aspect replacement MCP
   * @throws IOException if aspect fetch or JSON processing fails
   * @throws ExecutionException if future execution fails
   * @throws InterruptedException if waiting is interrupted
   */
  @Nonnull
  static MetadataChangeProposal transformPatchToFullAspect(
      @Nonnull MetadataChangeProposal patch, @Nonnull EntityClient client)
      throws IOException, ExecutionException, InterruptedException {

    log.debug(
        "Transforming tag patch to full aspect for entity: {}", patch.getEntityUrn().toString());

    // Step 1: Fetch current globalTags aspect
    datahub.client.v2.operations.AspectWithMetadata<GlobalTags> aspectWithMetadata =
        client.getAspect(patch.getEntityUrn(), GlobalTags.class);
    GlobalTags currentTags = aspectWithMetadata.getAspect();
    if (currentTags == null) {
      log.debug("No existing globalTags found, creating new aspect");
      currentTags = new GlobalTags();
      currentTags.setTags(new TagAssociationArray());
    } else {
      log.debug("Fetched existing globalTags with {} tags", currentTags.getTags().size());
    }

    // Step 2: Apply patch operations to current aspect
    GlobalTags updatedTags = applyPatchOperations(currentTags, patch);
    log.debug("After applying patch, have {} tags", updatedTags.getTags().size());

    // Step 3: Create full MCP with UPSERT
    MetadataChangeProposal fullMcp = new MetadataChangeProposal();
    fullMcp.setEntityType(patch.getEntityType());
    fullMcp.setEntityUrn(patch.getEntityUrn());
    fullMcp.setChangeType(ChangeType.UPSERT);
    fullMcp.setAspectName("globalTags");

    // Serialize the aspect to GenericAspect
    GenericAspect genericAspect = new GenericAspect();
    genericAspect.setValue(
        ByteString.copyString(
            RecordTemplate.class.cast(updatedTags).data().toString(), StandardCharsets.UTF_8));
    genericAspect.setContentType("application/json");
    fullMcp.setAspect(genericAspect);

    log.debug("Created full MCP for globalTags aspect");
    return fullMcp;
  }

  /**
   * Applies JSON Patch operations from a patch MCP to a GlobalTags aspect.
   *
   * <p>Handles ADD and REMOVE operations on the /tags/ path.
   *
   * <p>Package-private for testing.
   *
   * @param current the current GlobalTags aspect
   * @param patch the patch MCP containing operations
   * @return the updated GlobalTags aspect
   * @throws IOException if JSON processing fails
   */
  @Nonnull
  static GlobalTags applyPatchOperations(
      @Nonnull GlobalTags current, @Nonnull MetadataChangeProposal patch) throws IOException {

    // Parse the patch aspect as JSON to extract operations
    JsonNode patchNode =
        OBJECT_MAPPER.readTree(patch.getAspect().getValue().asString(StandardCharsets.UTF_8));
    JsonNode patchArray = patchNode.get("patch");

    if (patchArray == null || !patchArray.isArray()) {
      log.warn("Patch does not contain expected 'patch' array, returning current aspect unchanged");
      return current;
    }

    // Make a mutable copy of current tags
    TagAssociationArray tags = new TagAssociationArray(current.getTags());

    // Apply each operation
    for (JsonNode operation : patchArray) {
      String op = operation.get("op").asText();
      String path = operation.get("path").asText();

      if (path.startsWith("/tags/")) {
        if ("add".equals(op)) {
          // Extract tag URN from value
          JsonNode value = operation.get("value");
          String tagUrnString = value.get("tag").asText();
          try {
            TagUrn tagUrn = TagUrn.createFromString(tagUrnString);

            // Check if tag already exists
            boolean exists = tags.stream().anyMatch(assoc -> assoc.getTag().equals(tagUrn));

            if (!exists) {
              TagAssociation assoc = new TagAssociation();
              assoc.setTag(tagUrn);
              if (value.has("context")) {
                assoc.setContext(value.get("context").asText());
              }
              tags.add(assoc);
              log.debug("Added tag: {}", tagUrnString);
            }
          } catch (URISyntaxException e) {
            log.warn("Invalid tag URN in patch: {}", tagUrnString, e);
          }
        } else if ("remove".equals(op)) {
          // Extract tag URN from path (encoded in path after /tags/)
          String encodedTagUrn = path.substring("/tags/".length());
          // The tag URN is URL-encoded in the path, decode it
          String tagUrnString = java.net.URLDecoder.decode(encodedTagUrn, StandardCharsets.UTF_8);

          try {
            TagUrn tagUrn = TagUrn.createFromString(tagUrnString);
            tags.removeIf(assoc -> assoc.getTag().equals(tagUrn));
            log.debug("Removed tag: {}", tagUrnString);
          } catch (URISyntaxException e) {
            log.warn("Invalid tag URN in patch path: {}", tagUrnString, e);
          }
        }
      }
    }

    current.setTags(tags);
    return current;
  }
}
