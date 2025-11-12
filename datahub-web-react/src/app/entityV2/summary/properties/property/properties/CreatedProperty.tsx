import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
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
        return (
            <Popover content={formatTimestamp(timestamp, 'll LTS')} placement="top">
                <DateWithTooltip>
                    <Text color="gray">{formatTimestamp(timestamp, 'll')}</Text>
                </DateWithTooltip>
            </Popover>
        );
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
