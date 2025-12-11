/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components/macro';

import { ANTD_GRAY } from '@app/entity/shared/constants';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType } from '@types';

export const EntityTypeLabel = styled.div<{ showBorder?: boolean }>`
    font-size: 14px;
    color: ${ANTD_GRAY[8]};
    ${(props) =>
        props.showBorder &&
        `
        border-bottom: 1px solid ${ANTD_GRAY[4]};
        padding-bottom: 2px;
        `}
`;

const SubtypesDescription = styled.span`
    font-size: 12px;
    font-weight: 400;
    margin-left: 8px;
    color: ${ANTD_GRAY[7]};
`;

interface Props {
    entityType: EntityType;
}

export default function SectionHeader({ entityType }: Props) {
    const entityRegistry = useEntityRegistry();
    const isDatasetType = entityType === EntityType.Dataset;

    return (
        <EntityTypeLabel showBorder>
            {entityRegistry.getCollectionName(entityType)}
            {isDatasetType && <SubtypesDescription>tables, topics, views, and more</SubtypesDescription>}
        </EntityTypeLabel>
    );
}
