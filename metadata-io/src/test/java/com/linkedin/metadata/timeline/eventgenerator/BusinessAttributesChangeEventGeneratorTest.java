package com.linkedin.metadata.timeline.eventgenerator;

import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.businessattribute.BusinessAttributeAssociation;
import com.linkedin.businessattribute.BusinessAttributes;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.BusinessAttributeUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.SystemMetadata;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import mock.MockEntitySpec;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

public class BusinessAttributesChangeEventGeneratorTest extends AbstractTestNGSpringContextTests {

  private static Urn getSchemaFieldUrn() throws URISyntaxException {
    return Urn.createFromString(
        "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD),user_id)");
  }

  private static final String BUSINESS_ATTRIBUTE_URN =
      "urn:li:businessAttribute:cypressTestAttribute";

  private static AuditStamp getTestAuditStamp() throws URISyntaxException {
    return new AuditStamp()
        .setActor(Urn.createFromString("urn:li:corpuser:__datahub_system"))
        .setTime(1683829509553L);
  }

  private static Aspect<BusinessAttributes> getBusinessAttributes(
      BusinessAttributeAssociation association) {
    return new Aspect<>(
        new BusinessAttributes().setBusinessAttribute(association), new SystemMetadata());
  }

  private static Aspect<BusinessAttributes> getNullBusinessAttributes() {
    MockEntitySpec mockEntitySpec = new MockEntitySpec("schemaField");
    BusinessAttributes businessAttributes = new BusinessAttributes();
    final AspectSpec aspectSpec =
        mockEntitySpec.createAspectSpec(businessAttributes, Constants.BUSINESS_ATTRIBUTE_ASPECT);
    final RecordTemplate nullAspect =
        GenericRecordUtils.deserializeAspect(
            ByteString.copyString("{}", StandardCharsets.UTF_8), "application/json", aspectSpec);
    return new Aspect(nullAspect, new SystemMetadata());
  }

  @Test
  public void testBusinessAttributeAddition() throws Exception {
    BusinessAttributesChangeEventGenerator businessAttributesChangeEventGenerator =
        new BusinessAttributesChangeEventGenerator();

    Urn urn = getSchemaFieldUrn();
    String entity = "schemaField";
    String aspect = "businessAttributes";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<BusinessAttributes> from = getNullBusinessAttributes();
    Aspect<BusinessAttributes> to =
        getBusinessAttributes(
            new BusinessAttributeAssociation()
                .setBusinessAttributeUrn(new BusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN)));

    List<ChangeEvent> actual =
        businessAttributesChangeEventGenerator.getChangeEvents(
            urn, entity, aspect, from, to, auditStamp);
    assertEquals(1, actual.size());
    assertEquals(ChangeOperation.ADD.name(), actual.get(0).getOperation().name());
    assertEquals(getSchemaFieldUrn(), Urn.createFromString(actual.get(0).getEntityUrn()));
  }

  @Test
  public void testBusinessAttributeRemoval() throws Exception {
    BusinessAttributesChangeEventGenerator test = new BusinessAttributesChangeEventGenerator();

    Urn urn = getSchemaFieldUrn();
    String entity = "schemaField";
    String aspect = "businessAttributes";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<BusinessAttributes> from =
        getBusinessAttributes(
            new BusinessAttributeAssociation()
                .setBusinessAttributeUrn(new BusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN)));
    Aspect<BusinessAttributes> to = getNullBusinessAttributes();

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);
    assertEquals(1, actual.size());
    assertEquals(ChangeOperation.REMOVE.name(), actual.get(0).getOperation().name());
    assertEquals(getSchemaFieldUrn(), Urn.createFromString(actual.get(0).getEntityUrn()));
  }

  @Test
  public void testNoChange() throws Exception {
    BusinessAttributesChangeEventGenerator test = new BusinessAttributesChangeEventGenerator();

    Urn urn = getSchemaFieldUrn();
    String entity = "schemaField";
    String aspect = "businessAttributes";
    AuditStamp auditStamp = getTestAuditStamp();

    Aspect<BusinessAttributes> from =
        getBusinessAttributes(
            new BusinessAttributeAssociation()
                .setBusinessAttributeUrn(new BusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN)));
    Aspect<BusinessAttributes> to =
        getBusinessAttributes(
            new BusinessAttributeAssociation()
                .setBusinessAttributeUrn(new BusinessAttributeUrn(BUSINESS_ATTRIBUTE_URN)));

    List<ChangeEvent> actual = test.getChangeEvents(urn, entity, aspect, from, to, auditStamp);
    assertEquals(0, actual.size());
  }
}
