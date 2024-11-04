package com.linkedin.metadata.kafka.hook.user;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.Constants.PLATFORM_RESOURCE_INFO_ASPECT_NAME;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.SerializedValueContentType;
import com.linkedin.common.url.Url;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataplatform.slack.SlackUserInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.platformresource.PlatformResourceInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SlackMemberResolutionUtils {

  final SystemEntityClient entityClient;

  private static final String PLATFORM_RESOURCE_SECONDARY_KEYS_SEARCH_FIELD = "secondaryKeys";
  private static final String PLATFORM_RESOURCE_PRIMARY_KEYS_SEARCH_FIELD = "primaryKey";
  private static final String PLATFORM_RESOURCE_TYPE_KEYS_SEARCH_FIELD = "resourceType";

  private static final String SLACK_MEMBER_SCHEMA_REF = SlackUserInfo.class.getName();

  @VisibleForTesting
  // TODO: serialize from PlatformResourceType.pdl
  // NOTE: see ingestion > slack.py > ResourceType
  public static final String PLATFORM_RESOURCE_TYPE_KEYS_SEARCH_FIELD_VALUE = "user-info";

  public SlackMemberResolutionUtils(@Nonnull final SystemEntityClient entityClient) {
    this.entityClient = entityClient;
  }

  @Nullable
  public PlatformResourceInfo findSlackMemberWithEmailOrMemberID(
      @Nonnull OperationContext operationContext, @Nonnull String emailOrMemberID)
      throws Exception {
    final SearchResult results =
        entityClient.search(
            operationContext,
            PLATFORM_RESOURCE_ENTITY_NAME,
            "*",
            buildGetSlackMemberFilters(emailOrMemberID),
            null,
            0,
            1);
    if (!results.hasEntities() || results.getEntities().isEmpty()) {
      return null;
    }
    final SearchEntity result = results.getEntities().get(0);
    final Urn urn = result.getEntity();
    final EntityResponse response =
        entityClient.getV2(operationContext, urn, Set.of(PLATFORM_RESOURCE_INFO_ASPECT_NAME));
    if (response == null || !response.hasAspects()) {
      return null;
    }
    final EnvelopedAspect aspect = response.getAspects().get(PLATFORM_RESOURCE_INFO_ASPECT_NAME);
    if (aspect == null) {
      return null;
    }
    return new PlatformResourceInfo(aspect.getValue().data());
  }

  @Nonnull
  private Filter buildGetSlackMemberFilters(@Nonnull String emailOrMemberID) {
    final List<ConjunctiveCriterion> orConditions =
        ImmutableList.of(
            buildEqualsCriterion(PLATFORM_RESOURCE_SECONDARY_KEYS_SEARCH_FIELD, emailOrMemberID),
            buildEqualsCriterion(PLATFORM_RESOURCE_PRIMARY_KEYS_SEARCH_FIELD, emailOrMemberID));
    return new Filter().setOr(new ConjunctiveCriterionArray(orConditions));
  }

  @Nonnull
  private ConjunctiveCriterion buildEqualsCriterion(String field, String value) {
    return new ConjunctiveCriterion()
        .setAnd(
            new CriterionArray(
                ImmutableList.of(
                    CriterionUtils.buildCriterion(field, Condition.EQUAL, value),
                    CriterionUtils.buildCriterion(
                        PLATFORM_RESOURCE_TYPE_KEYS_SEARCH_FIELD,
                        Condition.EQUAL,
                        PLATFORM_RESOURCE_TYPE_KEYS_SEARCH_FIELD_VALUE))));
  }

  @Nullable
  public CorpUserEditableInfo fetchCorpUserEditableInfo(
      @Nonnull OperationContext operationContext, @Nonnull Urn userUrn) throws Exception {
    final EntityResponse response =
        entityClient.getV2(operationContext, userUrn, Set.of(CORP_USER_EDITABLE_INFO_ASPECT_NAME));
    if (response != null && response.hasAspects()) {
      final EnvelopedAspect envelopedAspect =
          response.getAspects().get(CORP_USER_EDITABLE_INFO_ASPECT_NAME);
      if (envelopedAspect != null) {
        return new CorpUserEditableInfo(envelopedAspect.getValue().data());
      }
    }
    return null;
  }

  public boolean upsertCorpUserAspectsWithSlackMemberDetails(
      @Nonnull OperationContext operationContext,
      @Nonnull Urn userUrn,
      @Nonnull PlatformResourceInfo info,
      @Nullable CorpUserEditableInfo editableInfo)
      throws Exception {
    if (!SerializedValueContentType.JSON.equals(info.getValue().getContentType())) {
      throw new Exception(String.format("Invalid value type %s", info.getValue().getContentType()));
    }
    if (!info.getValue().hasSchemaRef()) {
      throw new Exception("Missing value schema ref");
    }
    if (!info.getValue().getSchemaRef().equals(SLACK_MEMBER_SCHEMA_REF)) {
      throw new Exception(
          String.format(
              "Invalid value schema ref %s, expected %s",
              info.getValue().getSchemaRef(), SLACK_MEMBER_SCHEMA_REF));
    }
    final SlackUserInfo member =
        GenericRecordUtils.deserializeAspect(
            info.getValue().getBlob(), "application/json", SlackUserInfo.class);
    entityClient.ingestProposal(
        operationContext,
        AspectUtils.buildMetadataChangeProposal(userUrn, SLACK_USER_INFO, member),
        true);

    final CorpUserEditableInfo currentInfo =
        editableInfo != null ? editableInfo : new CorpUserEditableInfo();
    final CorpUserEditableInfo updatedInfo =
        this.coalesceSlackMemberToCorpUser(userUrn, currentInfo, member);

    // TODO: migrate to patch
    entityClient.ingestProposal(
        operationContext,
        AspectUtils.buildMetadataChangeProposal(
            userUrn, CORP_USER_EDITABLE_INFO_ASPECT_NAME, updatedInfo),
        false);
    return true;
  }

  // TODO: write tests
  @Nonnull
  public CorpUserEditableInfo coalesceSlackMemberToCorpUser(
      @Nonnull Urn userUrn, @Nonnull CorpUserEditableInfo infoRaw, @Nonnull SlackUserInfo member) {
    final CorpUserEditableInfo info = new CorpUserEditableInfo(infoRaw.data());
    // memberID
    info.setSlack(member.getId());

    // email
    if (!info.hasEmail() && member.hasEmail()) {
      info.setEmail(member.getEmail());
    }
    // display name
    if ((!info.hasDisplayName() || info.getDisplayName().equals(userUrn.getId()))
        && member.hasDisplayName()) {
      info.setDisplayName(member.getDisplayName());
    }
    // picture
    if ((!info.hasPictureLink()
            || info.getPictureLink().toString().startsWith("https://raw.githubusercontent.com"))
        && member.hasProfilePictureUrl()) {
      info.setPictureLink(new Url(member.getProfilePictureUrl()));
    }
    // phone
    if (!info.hasPhone() && member.hasPhone()) {
      info.setPhone(member.getPhone());
    }
    // title
    if (!info.hasTitle() && member.hasTitle()) {
      info.setTitle(member.getTitle());
    }
    return info;
  }
}
