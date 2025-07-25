import { useMemo } from 'react';

import {
    GROUP_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS_GROUP,
    GROUP_PROPOSAL_NOTIFICATIONS_GROUP,
} from '@app/settingsV2/personal/notifications/types';

import { AppConfig } from '@types';

export default function useGroupNotificationGroups(appConfig?: Partial<AppConfig> | null) {
    return useMemo(() => {
        const groups = [GROUP_PROPOSAL_NOTIFICATIONS_GROUP];

        if (appConfig?.featureFlags?.actionWorkflowsEnabled) {
            groups.push(GROUP_ACTION_WORKFLOW_FORM_REQUEST_NOTIFICATIONS_GROUP);
        }

        return groups;
    }, [appConfig?.featureFlags?.actionWorkflowsEnabled]);
}
