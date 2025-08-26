package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.settings.NotificationSettingMap;
import java.util.ArrayList;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NotificationSettingMapMapperTest {

  @Test
  public void testMapNotificationSettingWithoutParams() {
    QueryContext context = Mockito.mock(QueryContext.class);

    String typeStr = "PROPOSER_PROPOSAL_STATUS_CHANGE";

    // Create a notification setting with no params.
    com.linkedin.settings.NotificationSetting setting =
        new com.linkedin.settings.NotificationSetting();
    setting.setValue(com.linkedin.settings.NotificationSettingValue.ENABLED);
    // Note: No params are set so hasParams() should be false.

    // Call the mapper
    NotificationSetting result =
        NotificationSettingMapMapper.mapNotificationSetting(context, typeStr, setting);

    // Verify that the type and value are mapped correctly and params is null.
    Assert.assertEquals(
        result.getType(), NotificationScenarioType.valueOf("PROPOSER_PROPOSAL_STATUS_CHANGE"));
    Assert.assertEquals(result.getValue(), NotificationSettingValue.valueOf("ENABLED"));
    Assert.assertNull(result.getParams());
  }

  @Test
  public void testMapNotificationSettingWithParams() {
    QueryContext context = Mockito.mock(QueryContext.class);
    String typeStr = "PROPOSER_PROPOSAL_STATUS_CHANGE";

    com.linkedin.settings.NotificationSetting setting =
        new com.linkedin.settings.NotificationSetting();
    setting.setValue(com.linkedin.settings.NotificationSettingValue.DISABLED);

    // Create parameters
    StringMap params = new StringMap();
    params.put("key1", "value1");
    params.put("key2", "value2");
    setting.setParams(params);

    NotificationSetting result =
        NotificationSettingMapMapper.mapNotificationSetting(context, typeStr, setting);

    Assert.assertEquals(
        result.getType(), NotificationScenarioType.valueOf("PROPOSER_PROPOSAL_STATUS_CHANGE"));
    Assert.assertEquals(result.getValue(), NotificationSettingValue.valueOf("DISABLED"));
    Assert.assertNotNull(result.getParams());
    Assert.assertEquals(
        result.getParams().stream()
            .filter(entry -> entry.getKey().equals("key1"))
            .findFirst()
            .get()
            .getValue(),
        "value1");
    Assert.assertEquals(
        result.getParams().stream()
            .filter(entry -> entry.getKey().equals("key2"))
            .findFirst()
            .get()
            .getValue(),
        "value2");
  }

  @Test
  public void testMapNotificationSettings() {
    QueryContext context = Mockito.mock(QueryContext.class);
    NotificationSettingMap settingsMap = new NotificationSettingMap();

    // First notification setting without params
    com.linkedin.settings.NotificationSetting setting1 =
        new com.linkedin.settings.NotificationSetting();
    setting1.setValue(com.linkedin.settings.NotificationSettingValue.ENABLED);

    // Second notification setting with params
    com.linkedin.settings.NotificationSetting setting2 =
        new com.linkedin.settings.NotificationSetting();
    setting2.setValue(com.linkedin.settings.NotificationSettingValue.DISABLED);
    StringMap params = new StringMap();
    params.put("param", "value");
    setting2.setParams(params);

    // Put the settings into the map (key is the string representation of the scenario type)
    settingsMap.put("PROPOSAL_STATUS_CHANGE", setting1);
    settingsMap.put("PROPOSER_PROPOSAL_STATUS_CHANGE", setting2);

    List<NotificationSetting> results =
        NotificationSettingMapMapper.mapNotificationSettings(context, settingsMap);

    // Expecting two results
    Assert.assertEquals(results.size(), 2);

    // Validate each mapping (order is not guaranteed)
    for (NotificationSetting ns : results) {
      if (ns.getType() == NotificationScenarioType.valueOf("PROPOSAL_STATUS_CHANGE")) {
        Assert.assertEquals(ns.getValue(), NotificationSettingValue.valueOf("ENABLED"));
        Assert.assertNull(ns.getParams());
      } else if (ns.getType()
          == NotificationScenarioType.valueOf("PROPOSER_PROPOSAL_STATUS_CHANGE")) {
        Assert.assertEquals(ns.getValue(), NotificationSettingValue.valueOf("DISABLED"));
        Assert.assertNotNull(ns.getParams());
        Assert.assertEquals(
            ns.getParams().stream()
                .filter(entry -> entry.getKey().equals("param"))
                .findFirst()
                .get()
                .getValue(),
            "value");
      } else {
        Assert.fail("Unexpected notification type: " + ns.getType());
      }
    }
  }

  @Test
  public void testMapNotificationSettingInputList() {
    List<NotificationSettingInput> inputList = new ArrayList<>();

    NotificationSettingInput input1 = new NotificationSettingInput();
    input1.setType(NotificationScenarioType.PROPOSAL_STATUS_CHANGE);
    input1.setValue(NotificationSettingValue.ENABLED);
    List<StringMapEntryInput> params1 = new ArrayList<>();
    StringMapEntryInput entry1 = new StringMapEntryInput();
    entry1.setKey("foo");
    entry1.setValue("bar");
    params1.add(entry1);
    input1.setParams(params1);

    NotificationSettingInput input2 = new NotificationSettingInput();
    input2.setType(NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE);
    input2.setValue(NotificationSettingValue.DISABLED);

    inputList.add(input1);
    inputList.add(input2);

    NotificationSettingMap map =
        NotificationSettingMapMapper.mapNotificationSettingInputList(inputList);

    // Verify that the map has two entries.
    Assert.assertEquals(map.size(), 2);

    // Validate EMAIL mapping
    com.linkedin.settings.NotificationSetting settingEmail = map.get("PROPOSAL_STATUS_CHANGE");
    Assert.assertNotNull(settingEmail);
    Assert.assertEquals(settingEmail.getValue().toString(), "ENABLED");
    Assert.assertNotNull(settingEmail.getParams());
    Assert.assertEquals(settingEmail.getParams().get("foo"), "bar");

    // Validate SMS mapping
    com.linkedin.settings.NotificationSetting settingSMS =
        map.get("PROPOSER_PROPOSAL_STATUS_CHANGE");
    Assert.assertNotNull(settingSMS);
    Assert.assertEquals(settingSMS.getValue().toString(), "DISABLED");
    Assert.assertNull(settingSMS.getParams());
  }
}
