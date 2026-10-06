import { Text } from '@components';
import React from 'react';
import styled from 'styled-components';

import { useEntityContext } from '@app/entity/shared/EntityContext';
import { GenericEntityProperties } from '@app/entity/shared/types';
import BaseProperty from '@app/entityV2/summary/properties/property/properties/BaseProperty';
import { PropertyComponentProps } from '@app/entityV2/summary/properties/types';
import { formatTimestamp } from '@app/sharedV2/time/utils';
import { Popover } from '@src/alchemy-components';

import { Chart, Container, Dashboard, Dataset, Document, DocumentSourceType, EntityType, Metric } from '@types';

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

function getDocumentCreatedTimestamp(document: Document): number | undefined {
    const created = document?.info?.created?.time;
    const lastModified = document?.info?.lastModified?.time;
    // For external documents, only show created if it predates lastModified.
    // If created >= lastModified the connector likely defaulted to ingestion time.
    const isExternal = document?.info?.source?.sourceType === DocumentSourceType.External;
    if (!isExternal || (created && lastModified && created < lastModified)) {
        return created;
    }
    return undefined;
}

function getCreatedTimestamp(
    entityType: EntityType,
    entityData: GenericEntityProperties | null,
): number | null | undefined {
    switch (entityType) {
        case EntityType.Document:
            return getDocumentCreatedTimestamp(entityData as Document);
        case EntityType.Dataset:
            return (entityData as Dataset)?.properties?.created;
        case EntityType.Chart:
            return (entityData as Chart)?.properties?.created?.time;
        case EntityType.Dashboard:
            return (entityData as Dashboard)?.properties?.created?.time;
        case EntityType.Container:
            return (entityData as Container)?.properties?.created?.time;
        case EntityType.Metric:
            return (entityData as Metric)?.info?.created?.time;
        default:
            return entityData?.properties?.createdOn?.time;
    }
}

export default function CreatedProperty(props: PropertyComponentProps) {
    const { entityData, entityType, loading } = useEntityContext();

    const createdTimestamp = getCreatedTimestamp(entityType, entityData);

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
