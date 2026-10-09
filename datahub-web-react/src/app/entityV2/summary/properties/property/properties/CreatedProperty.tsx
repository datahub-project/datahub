import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';
import { Popover } from '@src/alchemy-components';

const DATE_TIME_FORMAT = 'll LTS';
const DATE_FORMAT = 'll';

const DateWithTooltip = styled.span`
    cursor: help;
    &:hover {
        text-decoration: underline;
        text-decoration-style: dotted;
        text-decoration-color: ${(props) => props.theme.colors.border};
    }
`;

export default function CreatedProperty(props: PropertyComponentProps) {
    const { entityData, entityType, loading } = useEntityContext();
    const entityRegistry = useEntityRegistryV2();

    const createdTimestamp =
        entityRegistry.getCreatedTime(entityType, entityData) ?? entityData?.properties?.createdOn?.time;

    const renderCreated = (timestamp: number) => {
        return (
            <Popover content={formatTimestamp(timestamp, DATE_TIME_FORMAT)} placement="top">
                <DateWithTooltip>
                    <Text>{formatTimestamp(timestamp, DATE_FORMAT)}</Text>
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
