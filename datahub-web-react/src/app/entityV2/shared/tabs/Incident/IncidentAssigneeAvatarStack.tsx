/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';

import { MAX_VISIBLE_ASSIGNEE } from '@app/entityV2/shared/tabs/Incident/constant';
import { AssigneeAvatarStackContainer } from '@app/entityV2/shared/tabs/Incident/styledComponents';
import { AvatarStack } from '@src/alchemy-components/components/AvatarStack/AvatarStack';

export const IncidentAssigneeAvatarStack = ({ assignees }: { assignees: any[] }) => {
    return (
        <AssigneeAvatarStackContainer data-testid="incident-avatar-stack">
            <AvatarStack avatars={assignees?.slice(0, MAX_VISIBLE_ASSIGNEE)} showRemainingNumber={false} />
            {assignees?.length > MAX_VISIBLE_ASSIGNEE && <span>{`+${assignees.length - MAX_VISIBLE_ASSIGNEE}`}</span>}
        </AssigneeAvatarStackContainer>
    );
};
