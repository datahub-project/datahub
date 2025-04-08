import React from 'react';

// import styled from 'styled-components/macro';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

import { QueryEntity } from '@types';

export default function SidebarQueryUpdatedAtSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();

    return (
        <SidebarSection
            title="Last Seen"
            content={<>{toRelativeTimeString(baseEntity?.entity?.properties?.lastModified?.time || 0)}</>}
        />
    );
}
