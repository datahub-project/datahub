import { Text } from '@src/alchemy-components';
import { MatchText } from '@src/alchemy-components/components/MatchText';
import { Entity } from '@src/types.generated';
import styled from 'styled-components';
import React from 'react';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import EntityIcon from './components/icon/EntityIcon';
import EntitySubtitle from './components/subtitle/EntitySubtitle';
import { getEntityDisplayType } from './utils';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    padding: 8px;
`;

const ContentContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 8px;
`;

const DescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    max-width: 400px;
`;

const EntityTitleContainer = styled.div``;

const IconContainer = styled.div`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
`;

const TypeContainer = styled.div`
    display: flex;
    align-items: center;
`;

interface EntityAutocompleteItemProps {
    entity: Entity;
    query?: string;
    siblings?: Entity[];
}

export default function AutoCompleteEntityItem({ entity, query, siblings }: EntityAutocompleteItemProps) {
    const entityRegistry = useEntityRegistryV2();
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const displayType = getEntityDisplayType(entity, entityRegistry);

    return (
        <Container>
            <ContentContainer>
                <IconContainer>
                    <EntityIcon entity={entity} siblings={siblings} />
                </IconContainer>

                <DescriptionContainer>
                    <EntityTitleContainer>
                        <MatchText text={displayName} highlight={query ?? ''} />
                    </EntityTitleContainer>

                    <EntitySubtitle entity={entity} />
                </DescriptionContainer>
            </ContentContainer>

            <TypeContainer>
                <Text color="gray" size="sm">
                    {displayType}
                </Text>
            </TypeContainer>
        </Container>
    );
}
