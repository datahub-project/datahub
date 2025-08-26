import { buildNotificationSettingsMap } from '@app/settingsV2/notifications/utils';
import { FormattedNotificationSetting } from '@src/app/settings/platform/types';

import { GlobalNotificationSettings, NotificationScenarioType } from '@types';

export const buildGlobalNotificationSettingsMap = (
    settings?: GlobalNotificationSettings | null,
): Map<NotificationScenarioType, FormattedNotificationSetting> => {
    return buildNotificationSettingsMap(settings?.settings);
};
