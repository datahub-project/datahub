import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityData } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';
import { Popover } from '@src/alchemy-components';

import { Document, EntityType } from '@types';

const DateWithTooltip = styled.span`
    cursor: help;
    &:hover {
        text-decoration: underline;
        text-decoration-style: dotted;
        text-decoration-color: #d9d9d9;
    }
`;

export default function LastUpdatedProperty(props: PropertyComponentProps) {
    const { entityData, entityType } = useEntityData();

    // Different entities store lastModified in different locations
    let lastModified: number | undefined;

    if (entityType === EntityType.Document) {
        const document = entityData as Document;
        lastModified = document?.info?.lastModified?.time;
    } else {
        // For other entities, check common locations
        // Using type assertion as different entities may have different property structures
        lastModified = (entityData as any)?.properties?.lastModified?.time;
    }

    const renderLastModified = (timestamp: number) => {
        return (
            <Popover content={formatTimestamp(timestamp, 'll LTS')} placement="top">
                <DateWithTooltip>
                    <Text color="gray">{formatTimestamp(timestamp, 'll')}</Text>
                </DateWithTooltip>
            </Popover>
        );
    };

    return <BaseProperty {...props} values={lastModified ? [lastModified] : []} renderValue={renderLastModified} />;
}
