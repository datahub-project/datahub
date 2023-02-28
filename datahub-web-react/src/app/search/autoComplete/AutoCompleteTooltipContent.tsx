import { FolderOpenOutlined } from '@ant-design/icons';
import React from 'react';
import styled from 'styled-components';
import { Entity, EntityType } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import { ArrowWrapper } from './ParentContainers';

const ContentWrapper = styled.div`
    font-size: 12px;
    color: white;
`;

const Container = styled.span`
    margin-left: 4px;
`;

const ContainersWrapper = styled.div`
    margin-top: 8px;
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
            <div>{displayName}</div>
            {parentContainers.length > 0 && (
                <ContainersWrapper>
                    {[...parentContainers].reverse().map((container, index) => (
                        <>
                            <FolderOpenOutlined />
                            <Container>{entityRegistry.getDisplayName(EntityType.Container, container)}</Container>
                            {index !== parentContainers.length - 1 && <ArrowWrapper>{'>'}</ArrowWrapper>}
                        </>
                    ))}
                </ContainersWrapper>
            )}
        </ContentWrapper>
    );
}
