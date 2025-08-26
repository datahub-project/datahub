package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.settings.NotificationSettingMap;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public class NotificationSettingMapMapper {
  public static List<NotificationSetting> mapNotificationSettings(
      @Nonnull final QueryContext context, NotificationSettingMap settings) {
    final List<NotificationSetting> result = new ArrayList<>();
    settings.forEach((key, value) -> result.add(mapNotificationSetting(context, key, value)));
    return result;
  }

  public static NotificationSetting mapNotificationSetting(
      @Nonnull final QueryContext context,
      String typeStr,
      com.linkedin.settings.NotificationSetting setting) {
    final NotificationSetting result = new NotificationSetting();
    result.setType(NotificationScenarioType.valueOf(typeStr));
    result.setValue(NotificationSettingValue.valueOf(setting.getValue().name()));
    if (setting.hasParams()) {
      result.setParams(StringMapMapper.map(context, setting.getParams()));
    }
    return result;
  }

  public static NotificationSettingMap mapNotificationSettingInputList(
      List<NotificationSettingInput> updatedSettings) {
    NotificationSettingMap map = new NotificationSettingMap();
    for (NotificationSettingInput input : updatedSettings) {
      com.linkedin.settings.NotificationSetting notificationSetting =
          new com.linkedin.settings.NotificationSetting();
      notificationSetting.setValue(
          com.linkedin.settings.NotificationSettingValue.valueOf(input.getValue().toString()));
      if (input.getParams() != null) {
        notificationSetting.setParams(mapParams(input.getParams()));
      }
      map.put(input.getType().toString(), notificationSetting);
    }
    return map;
  }

  private static StringMap mapParams(List<StringMapEntryInput> input) {
    final StringMap result = new StringMap();
    for (StringMapEntryInput entry : input) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
