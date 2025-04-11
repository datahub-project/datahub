import React from 'react';
// import styled from 'styled-components/macro';

import { useBaseEntity } from '../../../../../../entity/shared/EntityContext';
import { SidebarSection } from '../SidebarSection';
import { QueryEntity } from '../../../../../../../types.generated';
import { toRelativeTimeString } from '../../../../../../shared/time/timeUtils';

export default function SidebarQueryUpdatedAtSection() {
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();

    return (
        <SidebarSection
            title="Last Seen"
            content={<>{toRelativeTimeString(baseEntity?.entity?.properties?.lastModified?.time || 0)}</>}
        />
    );
}
