import { Text } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { CorpUser, Entity, EntityType } from '@src/types.generated';
import React, { useCallback } from 'react';
import styled from 'styled-components';
import EntityIcon from '@src/app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import { FilterComponentProps } from '../../types';
import BaseEntityFilter from '../BaseEntityFilter/BaseEntityFilter';

interface PlatformLabelProps {
    entity: Entity;
}

const Container = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    text-overflow: ellipsis;
    text-wrap: nowrap;
    align-items: center;
    justify-content: space-between;
`;

const IconAndNameContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
    text-overflow: ellipsis;
    text-wrap: nowrap;
    align-items: center;
`;

const IconWrapper = styled.div`
    display: flex;
    align-items: center;

    & .ant-image {
        display: flex;
        align-items: center;
    }
`;

const TitleContainer = styled.div`
    display: flex;
    flex-direction: column;
    /* gap: 8px; */
`;

function OwnerLabel({ entity }: PlatformLabelProps) {
    const entityRegistry = useEntityRegistryV2();

    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const subtitle = entity.type === EntityType.CorpUser ? (entity as CorpUser)?.properties?.email : undefined;

    return (
        <Container>
            <IconAndNameContainer>
                <IconWrapper>
                    <EntityIcon entity={entity} />
                </IconWrapper>
                <TitleContainer>
                    <Text type="div">{displayName}</Text>
                    {subtitle && (
                        <Text type="div" size="sm" color="gray">
                            {subtitle}
                        </Text>
                    )}
                </TitleContainer>
            </IconAndNameContainer>
        </Container>
    );
}

export default function OwnerFilter(props: FilterComponentProps) {
    const renderEntity = useCallback((entity: Entity) => <OwnerLabel entity={entity} />, []);

    return (
        <BaseEntityFilter
            {...props}
            renderEntity={renderEntity}
            entityTypes={[EntityType.CorpUser, EntityType.CorpGroup]}
            filterName="Owner"
        />
    );
}
