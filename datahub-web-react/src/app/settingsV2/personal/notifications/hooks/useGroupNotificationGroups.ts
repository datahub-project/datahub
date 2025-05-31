import { useMemo } from 'react';

import { GROUP_PROPOSAL_NOTIFICATIONS_GROUP } from '@app/settingsV2/personal/notifications/types';

import { AppConfig } from '@types';

export default function useGroupNotificationGroups(_appConfig?: Partial<AppConfig> | null) {
    return useMemo(() => {
        const groups = [GROUP_PROPOSAL_NOTIFICATIONS_GROUP];

        return groups;
    }, []);
}
