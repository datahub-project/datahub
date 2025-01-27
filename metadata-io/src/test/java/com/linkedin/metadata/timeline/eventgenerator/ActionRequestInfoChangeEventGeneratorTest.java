package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.*;

import com.linkedin.actionrequest.*;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import com.linkedin.util.Pair;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

public class ActionRequestInfoChangeEventGeneratorTest {

  @Test
  public void testCreateStructuredPropertyProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createStructuredPropertyActionRequestInfo(
            "urn:li:structuredProperty:test", Arrays.asList("test", "testing", "onemore"));

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals(
        "[{\"propertyUrn\":\"urn:li:structuredProperty:test\",\"values\":[\"test\",\"testing\",\"onemore\"]}]",
        actual.get(0).getParameters().get("structuredProperties"));
  }

  private ActionRequestInfo createStructuredPropertyActionRequestInfo(
      String propertyUrn, List<String> values) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("STRUCTURED_PROPERTY_ASSOCIATION");

    StructuredPropertyProposal proposal = new StructuredPropertyProposal();
    StructuredPropertyValueAssignmentArray array = new StructuredPropertyValueAssignmentArray();

    StructuredPropertyValueAssignment assignment = new StructuredPropertyValueAssignment();
    assignment.setPropertyUrn(Urn.createFromString(propertyUrn));

    PrimitivePropertyValueArray propValues = new PrimitivePropertyValueArray();
    for (String value : values) {
      propValues.add(PrimitivePropertyValue.create(value));
    }
    assignment.setValues(propValues);

    array.add(assignment);
    proposal.setStructuredPropertyValues(array);

    ActionRequestParams params = new ActionRequestParams();
    params.setStructuredPropertyProposal(proposal);
    info.setParams(params);

    return info;
  }

  @Test
  public void testCreateDomainProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createDomainActionRequestInfo("urn:li:domain:engineering");

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals("[\"urn:li:domain:engineering\"]", actual.get(0).getParameters().get("domains"));
  }

  @Test
  public void testCreateOwnerProposal() throws Exception {
    ActionRequestInfoChangeEventGenerator test = new ActionRequestInfoChangeEventGenerator();

    Urn urn = Urn.createFromString("urn:li:actionRequest:test");
    String entity = "actionRequest";
    String aspect = "actionRequestInfo";
    AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
            .setTime(1683829509553L);

    ActionRequestInfo actionRequestInfo =
        createOwnerActionRequestInfo(
            Arrays.asList(
                new Pair<>(
                    "urn:li:corpuser:admin",
                    new Pair<>("TECHNICAL_OWNER", "urn:li:ownershipType:technicalOwner")),
                new Pair<>(
                    "urn:li:corpGroup:my_team",
                    new Pair<>("BUSINESS_OWNER", "urn:li:ownershipType:businessOwner"))));

    Aspect<ActionRequestInfo> from = new Aspect<>(null, new SystemMetadata());
    Aspect<ActionRequestInfo> to = new Aspect<>(actionRequestInfo, new SystemMetadata());

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);

    assertEquals(1, actual.size());
    assertEquals("CREATE", actual.get(0).getOperation().toString());
    assertEquals(
        "[{\"type\":\"TECHNICAL_OWNER\",\"typeUrn\":\"urn:li:ownershipType:technicalOwner\",\"ownerUrn\":\"urn:li:corpuser:admin\"},"
            + "{\"type\":\"BUSINESS_OWNER\",\"typeUrn\":\"urn:li:ownershipType:businessOwner\",\"ownerUrn\":\"urn:li:corpGroup:my_team\"}]",
        actual.get(0).getParameters().get("owners"));
  }

  private ActionRequestInfo createDomainActionRequestInfo(String domainUrn) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("DOMAIN_ASSOCIATION");

    DomainProposal proposal = new DomainProposal();
    UrnArray domains = new UrnArray();
    domains.add(Urn.createFromString(domainUrn));
    proposal.setDomains(domains);

    ActionRequestParams params = new ActionRequestParams();
    params.setDomainProposal(proposal);
    info.setParams(params);

    return info;
  }

  private ActionRequestInfo createOwnerActionRequestInfo(
      List<Pair<String, Pair<String, String>>> ownerInfo) throws Exception {
    ActionRequestInfo info = new ActionRequestInfo();
    info.setType("OWNER_ASSOCIATION");

    OwnerProposal proposal = new OwnerProposal();
    OwnerArray owners = new OwnerArray();

    for (Pair<String, Pair<String, String>> pair : ownerInfo) {
      Owner owner = new Owner();
      owner.setOwner(Urn.createFromString(pair.getFirst()));
      owner.setType(Enum.valueOf(OwnershipType.class, pair.getSecond().getFirst()));
      owner.setTypeUrn(Urn.createFromString(pair.getSecond().getSecond()));
      owners.add(owner);
    }

    proposal.setOwners(owners);

    ActionRequestParams params = new ActionRequestParams();
    params.setOwnerProposal(proposal);
    info.setParams(params);

    return info;
  }
}
