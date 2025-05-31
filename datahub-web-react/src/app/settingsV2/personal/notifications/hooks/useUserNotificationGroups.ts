import { useMemo } from 'react';

import {
    USER_COMPLIANCE_FORMS_NOTIFICATIONS_GROUP,
    USER_PROPOSAL_NOTIFICATIONS_GROUP,
} from '@app/settingsV2/personal/notifications/types';

import { AppConfig } from '@types';

export default function useUserNotificationGroups(appConfig?: Partial<AppConfig> | null) {
    return useMemo(() => {
        const groups = [USER_PROPOSAL_NOTIFICATIONS_GROUP];

        if (appConfig?.featureFlags?.showNotificationSettingsForComplianceForms) {
            groups.push(USER_COMPLIANCE_FORMS_NOTIFICATIONS_GROUP);
        }

        return groups;
    }, [appConfig]);
}
