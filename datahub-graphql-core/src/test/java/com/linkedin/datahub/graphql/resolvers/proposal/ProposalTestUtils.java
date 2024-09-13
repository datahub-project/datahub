package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;

public class ProposalTestUtils {
  public static final Urn TAG_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:tag");
  public static final Urn TERM_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:term");
  public static final Urn DESCRIPTION_ACTION_REQUEST =
      UrnUtils.getUrn("urn:li:actionRequest:description");
  public static final Urn ACCEPTED_ACTION_REQUEST =
      UrnUtils.getUrn("urn:li:actionRequest:accepted");
  public static final Urn TEST_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  public static final Urn TEST_GLOSSARY_TERM = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  public static final Urn TEST_TAG = UrnUtils.getUrn("urn:li:tag:haha");
  public static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  public static Snapshot buildMockTagProposalSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_TAG_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams().setTagProposal(new TagProposal().setTag(TEST_TAG))));

    ActionRequestAspect statusAspect = new ActionRequestAspect();
    statusAspect.setActionRequestStatus(
        new ActionRequestStatus()
            .setStatus("PENDING")
            .setResult("SUCCESS")
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN)));

    actionRequestSnapshot.setAspects(
        new ActionRequestAspectArray(ImmutableList.of(infoAspect, statusAspect)));

    Snapshot snapshot = new Snapshot();
    snapshot.setActionRequestSnapshot(actionRequestSnapshot);
    return snapshot;
  }

  public static Snapshot buildMockTermProposalSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_TERM_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setGlossaryTermProposal(
                        new GlossaryTermProposal().setGlossaryTerm(TEST_GLOSSARY_TERM))));

    ActionRequestAspect statusAspect = new ActionRequestAspect();
    statusAspect.setActionRequestStatus(
        new ActionRequestStatus()
            .setStatus("PENDING")
            .setResult("SUCCESS")
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN)));

    actionRequestSnapshot.setAspects(
        new ActionRequestAspectArray(ImmutableList.of(infoAspect, statusAspect)));

    Snapshot snapshot = new Snapshot();
    snapshot.setActionRequestSnapshot(actionRequestSnapshot);
    return snapshot;
  }

  public static Snapshot buildMockDocumentationSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setUpdateDescriptionProposal(
                        new DescriptionProposal().setDescription("test description"))));

    ActionRequestAspect statusAspect = new ActionRequestAspect();
    statusAspect.setActionRequestStatus(
        new ActionRequestStatus()
            .setStatus("PENDING")
            .setResult("SUCCESS")
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN)));

    actionRequestSnapshot.setAspects(
        new ActionRequestAspectArray(ImmutableList.of(infoAspect, statusAspect)));

    Snapshot snapshot = new Snapshot();
    snapshot.setActionRequestSnapshot(actionRequestSnapshot);
    return snapshot;
  }

  public static Snapshot buildMockAcceptedSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setUpdateDescriptionProposal(
                        new DescriptionProposal().setDescription("test description"))));

    ActionRequestAspect statusAspect = new ActionRequestAspect();
    statusAspect.setActionRequestStatus(
        new ActionRequestStatus()
            .setStatus("ACCEPTED")
            .setResult("SUCCESS")
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN)));

    actionRequestSnapshot.setAspects(
        new ActionRequestAspectArray(ImmutableList.of(infoAspect, statusAspect)));

    Snapshot snapshot = new Snapshot();
    snapshot.setActionRequestSnapshot(actionRequestSnapshot);
    return snapshot;
  }

  private ProposalTestUtils() {}
}
