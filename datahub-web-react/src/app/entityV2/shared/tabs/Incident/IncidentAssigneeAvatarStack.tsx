import React from 'react';
import { AvatarStack } from '@src/alchemy-components/components/AvatarStack/AvatarStack';
import { MAX_VISIBLE_ASSIGNEE } from './constant';
import { AssigneeAvatarStackContainer } from './styledComponents';

export const IncidentAssigneeAvatarStack = ({ assignees }: { assignees: any[] }) => {
    return (
        <AssigneeAvatarStackContainer data-testid="incident-avatar-stack">
            <AvatarStack avatars={assignees?.slice(0, MAX_VISIBLE_ASSIGNEE)} />
            {assignees?.length > MAX_VISIBLE_ASSIGNEE && <span>{`+${assignees.length - MAX_VISIBLE_ASSIGNEE}`}</span>}
        </AssigneeAvatarStackContainer>
    );
};
