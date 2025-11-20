import { Text } from '@components';
import React from 'react';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';

export default function CreatedProperty(props: PropertyComponentProps) {
    const { entityData, loading } = useEntityContext();

    const createdTimestamp = entityData?.properties?.createdOn?.time;

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
