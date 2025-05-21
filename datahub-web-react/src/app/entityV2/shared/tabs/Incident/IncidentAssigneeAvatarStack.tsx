import React from 'react';

import { MAX_VISIBLE_ASSIGNEE } from '@app/entityV2/shared/tabs/Incident/constant';
import { AssigneeAvatarStackContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { AvatarStack } from '@src/alchemy-components/components/AvatarStack/AvatarStack';

export const IncidentAssigneeAvatarStack = ({ assignees }: { assignees: any[] }) => {
    return (
        <AssigneeAvatarStackContainer data-testid="incident-avatar-stack">
            <AvatarStack avatars={assignees?.slice(0, MAX_VISIBLE_ASSIGNEE)} />
            {assignees?.length > MAX_VISIBLE_ASSIGNEE && <span>{`+${assignees.length - MAX_VISIBLE_ASSIGNEE}`}</span>}
        </AssigneeAvatarStackContainer>
    );
};
