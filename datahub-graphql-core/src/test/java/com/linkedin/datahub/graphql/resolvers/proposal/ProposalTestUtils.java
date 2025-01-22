package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_OWNER_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TAG_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_TERM_PROPOSAL;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_UPDATE_DESCRIPTION_PROPOSAL;

import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionrequest.DescriptionProposal;
import com.linkedin.actionrequest.DomainProposal;
import com.linkedin.actionrequest.GlossaryTermProposal;
import com.linkedin.actionrequest.OwnerProposal;
import com.linkedin.actionrequest.StructuredPropertyProposal;
import com.linkedin.actionrequest.TagProposal;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.OwnershipType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.aspect.ActionRequestAspectArray;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import java.util.List;

public class ProposalTestUtils {
  public static final Urn LEGACY_TAG_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:tag");

  // New tags action request allows multiple tags to be proposed at once.
  public static final Urn TAG_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:tags-new");

  public static final Urn LEGACY_TERM_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:term");
  public static final Urn TERM_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:term-new");

  public static final Urn DESCRIPTION_ACTION_REQUEST =
      UrnUtils.getUrn("urn:li:actionRequest:description");
  public static final Urn STRUCTURED_PROPERTY_ACTION_REQUEST =
      UrnUtils.getUrn("urn:li:actionRequest:structuredProperty");

  public static final Urn DOMAIN_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:domain");
  public static final Urn OWNER_ACTION_REQUEST = UrnUtils.getUrn("urn:li:actionRequest:owner");

  public static final Urn ACCEPTED_ACTION_REQUEST =
      UrnUtils.getUrn("urn:li:actionRequest:accepted");
  public static final Urn TEST_DATASET =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:test,test,PROD)");
  public static final Urn TEST_GLOSSARY_TERM = UrnUtils.getUrn("urn:li:glossaryTerm:test");
  public static final Urn TEST_GLOSSARY_TERM_2 = UrnUtils.getUrn("urn:li:glossaryTerm:test-2");
  public static final Urn TEST_TAG = UrnUtils.getUrn("urn:li:tag:haha");
  public static final Urn TEST_TAG_2 = UrnUtils.getUrn("urn:li:tag:lol");
  public static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  public static final Urn TEST_OWNER_TYPE_URN = UrnUtils.getUrn("urn:li:ownershipType:test");

  public static Snapshot buildLegacyMockTagProposalSnapshot(Urn urn) {
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
                new ActionRequestParams()
                    .setTagProposal(
                        new TagProposal()
                            .setTags(new UrnArray(ImmutableList.of(TEST_TAG, TEST_TAG_2))))));

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

  public static Snapshot buildMockLegacyTermProposalSnapshot(Urn urn) {
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
                        new GlossaryTermProposal()
                            .setGlossaryTerms(
                                new UrnArray(
                                    ImmutableList.of(TEST_GLOSSARY_TERM, TEST_GLOSSARY_TERM_2))))));

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

  public static Snapshot buildMockStructuredPropertySnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    List<StructuredPropertyValueAssignment> propertyValueAssignments =
        ImmutableList.of(
            new StructuredPropertyValueAssignment()
                .setPropertyUrn(UrnUtils.getUrn("urn:li:structuredProperty:property1"))
                .setValues(
                    new PrimitivePropertyValueArray(
                        ImmutableList.of(PrimitivePropertyValue.create("stringval")))));

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_STRUCTURED_PROPERTY_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setStructuredPropertyProposal(
                        new StructuredPropertyProposal()
                            .setStructuredPropertyValues(
                                new StructuredPropertyValueAssignmentArray(
                                    propertyValueAssignments)))));

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

  public static Snapshot buildMockDomainSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    final List<Urn> domainUrns =
        ImmutableList.of(
            UrnUtils.getUrn("urn:li:domain:test"), UrnUtils.getUrn("urn:li:domain:test-2"));

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_DOMAIN_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setDomainProposal(new DomainProposal().setDomains(new UrnArray(domainUrns)))));

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

  public static Snapshot buildMockOwnerSnapshot(Urn urn) {
    ActionRequestSnapshot actionRequestSnapshot = new ActionRequestSnapshot();
    actionRequestSnapshot.setUrn(urn);

    List<Owner> owners =
        ImmutableList.of(
            new Owner().setOwner(TEST_ACTOR_URN).setType(OwnershipType.TECHNICAL_OWNER),
            new Owner()
                .setOwner(TEST_ACTOR_URN)
                .setType(OwnershipType.CUSTOM)
                .setTypeUrn(TEST_OWNER_TYPE_URN));

    ActionRequestAspect infoAspect = new ActionRequestAspect();
    infoAspect.setActionRequestInfo(
        new ActionRequestInfo()
            .setType(ACTION_REQUEST_TYPE_OWNER_PROPOSAL)
            .setResource(TEST_DATASET.toString())
            .setResourceType("dataset")
            .setParams(
                new ActionRequestParams()
                    .setOwnerProposal(new OwnerProposal().setOwners(new OwnerArray(owners)))));

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
