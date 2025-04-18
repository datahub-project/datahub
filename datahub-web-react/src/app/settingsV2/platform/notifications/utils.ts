import { FormattedNotificationSetting } from '@src/app/settings/platform/types';

import { GlobalNotificationSettings, NotificationScenarioType } from '../../../../types.generated';
import { buildNotificationSettingsMap } from '../../notifications/utils';

export const buildGlobalNotificationSettingsMap = (
    settings?: GlobalNotificationSettings | null,
): Map<NotificationScenarioType, FormattedNotificationSetting> => {
    return buildNotificationSettingsMap(settings?.settings);
};
