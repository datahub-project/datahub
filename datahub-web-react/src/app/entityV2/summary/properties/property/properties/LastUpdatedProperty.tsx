import { Text } from '@components';
import React from 'react';

import { useEntityData } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';

import { Document, EntityType } from '@types';

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
        return <Text color="gray">{formatTimestamp(timestamp, 'll')}</Text>;
    };

    return <BaseProperty {...props} values={lastModified ? [lastModified] : []} renderValue={renderLastModified} />;
}
