import React from 'react';
import { useTranslation } from 'react-i18next';

// import styled from 'styled-components/macro';

import { useBaseEntity } from '@app/entity/shared/EntityContext';
import { SidebarSection } from '@app/entityV2/shared/containers/profile/sidebar/SidebarSection';
import { toRelativeTimeString } from '@app/shared/time/timeUtils';

import { QueryEntity } from '@types';

export default function SidebarQueryUpdatedAtSection() {
    const { t } = useTranslation('entity.shared.containers');
    const baseEntity = useBaseEntity<{ entity: QueryEntity }>();

    return (
        <SidebarSection
            title={t('sidebar.query.lastSeenTitle')}
            content={<>{toRelativeTimeString(baseEntity?.entity?.properties?.lastModified?.time || 0)}</>}
        />
    );
}
