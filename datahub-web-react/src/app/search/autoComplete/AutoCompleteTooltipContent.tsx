import { FolderOpenOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
// eslint-disable-next-line @typescript-eslint/no-unused-vars
import { Dataset, Entity, EntityType } from '../../../types.generated';
import { DatasetStatsSummary } from '../../entity/dataset/shared/DatasetStatsSummary';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ArrowWrapper } from './ParentContainers';

const ContentWrapper = styled.div`
    font-size: 12px;
    color: white;
`;

const Container = styled.span`
    margin-left: 4px;
`;

const EntityName = styled.div`
    font-size: 14px;
`;

interface Props {
    entity: Entity;
}

export default function AutoCompleteTooltipContent({ entity }: Props) {
    const entityRegistry = useEntityRegistry();
    const genericEntityProps = entityRegistry.getGenericEntityProperties(entity.type, entity);
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const parentContainers = genericEntityProps?.parentContainers?.containers || [];

    return (
        <ContentWrapper>
            {parentContainers.length > 0 && (
                <>
                    {[...parentContainers].reverse().map((container, index) => (
                        <>
                            <FolderOpenOutlined />
                            <Container>{entityRegistry.getDisplayName(EntityType.Container, container)}</Container>
                            {index !== parentContainers.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    ))}
                </>
            )}
            <EntityName>{displayName}</EntityName>
            {entity.type === EntityType.Dataset && (
                <DatasetStatsSummary
                    rowCount={2133440}
                    columnCount={12}
                    sizeInBytes={29321728}
                    lastUpdatedMs={
                        (entity as any).lastOperation?.length && (entity as any).lastOperation[0].lastUpdatedTimestamp
                    }
                    queryCountLast30Days={172}
                    uniqueUserCountLast30Days={1}
                    mode="tooltip-content"
                />
            )}
        </ContentWrapper>
    );
}
