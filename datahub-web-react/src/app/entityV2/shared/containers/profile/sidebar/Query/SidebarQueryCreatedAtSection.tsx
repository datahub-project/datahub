/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
            title="First Seen"
            content={<>{toRelativeTimeString(baseEntity?.entity?.properties?.created?.time || 0)}</>}
        />
    );
}
