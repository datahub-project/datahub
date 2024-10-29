import { FolderOpenOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { Dataset, Entity, EntityType } from '../../../types.generated';
import { DatasetStatsSummary } from '../../entity/dataset/shared/DatasetStatsSummary';
import { getLastUpdatedMs } from '../../entity/dataset/shared/utils';
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
                    rowCount={(entity as any).lastProfile?.length && (entity as any).lastProfile[0].rowCount}
                    columnCount={(entity as any).lastProfile?.length && (entity as any).lastProfile[0].columnCount}
                    sizeInBytes={(entity as any).lastProfile?.length && (entity as any).lastProfile[0].sizeInBytes}
                    lastUpdatedMs={getLastUpdatedMs((entity as any)?.properties, (entity as any)?.lastOperation)}
                    queryCountLast30Days={(entity as Dataset).statsSummary?.queryCountLast30Days}
                    uniqueUserCountLast30Days={(entity as Dataset).statsSummary?.uniqueUserCountLast30Days}
                    mode="tooltip-content"
                />
            )}
        </ContentWrapper>
    );
}
