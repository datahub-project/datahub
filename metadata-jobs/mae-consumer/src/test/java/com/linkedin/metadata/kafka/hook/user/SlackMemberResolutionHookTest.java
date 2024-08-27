package com.linkedin.metadata.kafka.hook.user;

import com.datahub.util.RecordUtils;
import com.linkedin.common.SerializedValue;
import com.linkedin.common.SerializedValueContentType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.ByteString;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataplatform.slack.SlackUserInfo;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.platformresource.PlatformResourceInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SlackMemberResolutionHookTest {

  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final String TEST_USER_EMAIL = "email";
  private static final String TEST_SLACK_MEMBER_ID = "testMemberID";

  @Mock private SystemEntityClient entityClient;
  @Mock private SlackMemberResolutionUtils memberResolutionUtils;
  @Mock private OperationContext operationContext;

  private SlackMemberResolutionHook slackMemberResolutionHook;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    this.slackMemberResolutionHook =
        new SlackMemberResolutionHook(entityClient, true, "testSuffix", memberResolutionUtils);
    this.slackMemberResolutionHook.init(operationContext);
  }

  @Test
  public void testInvokeWithEligibleEventAndMissingSlackNotificationSettings() throws Exception {
    // Given
    final MetadataChangeLog event = createEligibleSettingsUpsertEvent();
    final CorpUserEditableInfo mockEditableInfo = createMockEditableInfo(false);
    Mockito.when(
            memberResolutionUtils.fetchCorpUserEditableInfo(
                Mockito.any(OperationContext.class), Mockito.eq(TEST_USER_URN)))
        .thenReturn(mockEditableInfo);
    Mockito.when(
            memberResolutionUtils.findSlackMemberWithEmailOrMemberID(
                Mockito.any(OperationContext.class), Mockito.eq(TEST_USER_EMAIL)))
        .thenReturn(createMockPlatformResourceInfo());
    Mockito.when(
            memberResolutionUtils.upsertCorpUserAspectsWithSlackMemberDetails(
                Mockito.any(OperationContext.class),
                Mockito.eq(TEST_USER_URN),
                Mockito.eq(createMockPlatformResourceInfo()),
                Mockito.eq(mockEditableInfo)))
        .thenReturn(true);

    // When
    slackMemberResolutionHook.invoke(event);

    // Then
    Mockito.verify(memberResolutionUtils, Mockito.times(1))
        .fetchCorpUserEditableInfo(Mockito.any(OperationContext.class), Mockito.eq(TEST_USER_URN));
    Mockito.verify(memberResolutionUtils, Mockito.times(1))
        .upsertCorpUserAspectsWithSlackMemberDetails(
            Mockito.any(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(createMockPlatformResourceInfo()),
            Mockito.eq(mockEditableInfo));
  }

  @Test
  public void testInvokeWithEligibleEventButHasSlackSettings() throws Exception {
    // Given
    MetadataChangeLog event = createEligibleEventWithoutSlackSettings();
    Mockito.when(
            memberResolutionUtils.fetchCorpUserEditableInfo(
                Mockito.any(OperationContext.class), Mockito.eq(TEST_USER_URN)))
        .thenReturn(createMockEditableInfo(false));
    ;

    // When
    slackMemberResolutionHook.invoke(event);

    // Then
    Mockito.verify(memberResolutionUtils, Mockito.times(1))
        .fetchCorpUserEditableInfo(Mockito.any(OperationContext.class), Mockito.eq(TEST_USER_URN));
    Mockito.verify(memberResolutionUtils, Mockito.times(0))
        .findSlackMemberWithEmailOrMemberID(
            Mockito.any(OperationContext.class), Mockito.anyString());
    Mockito.verify(memberResolutionUtils, Mockito.times(0))
        .upsertCorpUserAspectsWithSlackMemberDetails(
            Mockito.any(OperationContext.class), Mockito.any(), Mockito.any(), Mockito.any());
  }

  @Test
  public void testInvokeWithIneligibleEvent() throws Exception {
    // Given
    MetadataChangeLog event = createIneligibleEvent();

    // When
    slackMemberResolutionHook.invoke(event);

    // Then
    Mockito.verify(entityClient, Mockito.times(0))
        .getV2(Mockito.any(OperationContext.class), Mockito.any(), Mockito.anySet());
    Mockito.verify(entityClient, Mockito.times(0))
        .ingestProposal(Mockito.any(OperationContext.class), Mockito.any(), Mockito.eq(false));
  }

  // Utility methods to create mock data

  /** Create and return a MetadataChangeLog instance with Email settings */
  private MetadataChangeLog createEligibleSettingsUpsertEvent() {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName(Constants.CORP_USER_SETTINGS_ASPECT_NAME);
    CorpUserSettings settings =
        new CorpUserSettings()
            .setNotificationSettings(
                new NotificationSettings()
                    .setEmailSettings(new EmailNotificationSettings().setEmail(TEST_USER_EMAIL)));
    // Set Slack-related fields in slackSettings as needed
    GenericAspect aspect = GenericRecordUtils.serializeAspect(settings);
    event.setAspect(aspect);
    event.setEntityUrn(TEST_USER_URN);
    event.setEntityType(TEST_USER_URN.getEntityType());
    event.setChangeType(ChangeType.UPSERT);
    return event;
  }

  /** Create and return a MetadataChangeLog instance without Email settings */
  private MetadataChangeLog createEligibleEventWithoutSlackSettings() {
    final MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName(Constants.CORP_USER_SETTINGS_ASPECT_NAME);
    CorpUserSettings settings = new CorpUserSettings();
    GenericAspect aspect = GenericRecordUtils.serializeAspect(settings);
    event.setAspect(aspect);
    event.setEntityUrn(TEST_USER_URN);
    event.setEntityType(TEST_USER_URN.getEntityType());
    event.setChangeType(ChangeType.UPSERT);
    return event;
  }

  private MetadataChangeLog createIneligibleEvent() {
    final MetadataChangeLog event = new MetadataChangeLog();
    event.setAspectName(Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME);
    CorpUserInfo info = new CorpUserInfo();
    GenericAspect aspect = GenericRecordUtils.serializeAspect(info);
    event.setAspect(aspect);
    event.setEntityUrn(TEST_USER_URN);
    event.setEntityType(TEST_USER_URN.getEntityType());
    event.setChangeType(ChangeType.UPSERT);
    return event;
  }

  private CorpUserEditableInfo createMockEditableInfo(boolean hasSlack) {
    final CorpUserEditableInfo editableInfo = new CorpUserEditableInfo();
    if (hasSlack) {
      editableInfo.setSlack(TEST_SLACK_MEMBER_ID);
    }
    return editableInfo;
  }

  private SlackUserInfo createMockSlackMember() {
    final SlackUserInfo slackMember = new SlackUserInfo();
    slackMember.setEmail(TEST_USER_EMAIL);
    slackMember.setDisplayName("User");
    slackMember.setId(TEST_SLACK_MEMBER_ID);
    return slackMember;
  }

  private PlatformResourceInfo createMockPlatformResourceInfo() {
    final SlackUserInfo mockMember = createMockSlackMember();
    final String memberJSON = RecordUtils.toJsonString(mockMember);

    final PlatformResourceInfo resourceInfo = new PlatformResourceInfo();
    resourceInfo.setResourceType(
        SlackMemberResolutionUtils.PLATFORM_RESOURCE_TYPE_KEYS_SEARCH_FIELD_VALUE);
    resourceInfo.setPrimaryKey(TEST_SLACK_MEMBER_ID);
    resourceInfo.setSecondaryKeys(new StringArray(List.of(TEST_USER_EMAIL)));
    resourceInfo.setValue(
        new SerializedValue()
            .setContentType(SerializedValueContentType.JSON)
            .setSchemaRef(SlackUserInfo.class.getName())
            .setBlob(ByteString.copyString(memberJSON, StandardCharsets.UTF_8)));
    return resourceInfo;
  }
}
