import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';
import { Popover } from '@src/alchemy-components';

import { Document, DocumentSourceType, EntityType } from '@types';

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

export default function LastIngestedProperty(props: PropertyComponentProps) {
    const { entityData, loading, entityType } = useEntityContext();

    // Documents: only show for externally ingested docs (native ones get a misleading creation stamp).
    // All other entity types (SemanticModel, Metric, Dataset, etc.): show whenever lastIngested is set.
    const isEligible =
        entityType !== EntityType.Document ||
        (entityData as Document)?.info?.source?.sourceType === DocumentSourceType.External;
    const lastIngested = isEligible ? (entityData?.lastIngested ?? undefined) : undefined;

    const renderLastIngested = (timestamp: number) => {
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
            values={lastIngested ? [lastIngested] : []}
            renderValue={renderLastIngested}
            loading={loading}
        />
    );
}
