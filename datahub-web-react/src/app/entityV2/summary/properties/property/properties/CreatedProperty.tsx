import { Text } from '@components';
import React from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';

import { Document, EntityType } from '@types';

export default function CreatedProperty(props: PropertyComponentProps) {
    const { entityData, entityType, loading } = useEntityContext();

    // Different entities store created timestamp in different locations
    let createdTimestamp: number | undefined;

    if (entityType === EntityType.Document) {
        const document = entityData as Document;
        createdTimestamp = document?.info?.created?.time;
    } else {
        createdTimestamp = entityData?.properties?.createdOn?.time;
    }

    const renderCreated = (timestamp: number) => {
        return <Text color="gray">{formatTimestamp(timestamp, 'll')}</Text>;
    };

    return (
        <BaseProperty
            {...props}
            values={createdTimestamp ? [createdTimestamp] : []}
            renderValue={renderCreated}
            loading={loading}
        />
    );
}
