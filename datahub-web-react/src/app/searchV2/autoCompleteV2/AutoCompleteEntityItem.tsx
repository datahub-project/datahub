import React from 'react';
import styled from 'styled-components';

import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import Matches from '@app/searchV2/autoCompleteV2/components/matches/Matches';
import EntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/EntitySubtitle';
import { NAME_COLOR, NAME_COLOR_LEVEL, TYPE_COLOR, TYPE_COLOR_LEVEL } from '@app/searchV2/autoCompleteV2/constants';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import { Text } from '@src/alchemy-components';
import { MatchText } from '@src/alchemy-components/components/MatchText';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, MatchedField } from '@src/types.generated';

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
    overflow: hidden;
    width: 100%;
`;

const DescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    width: 100%;
`;

const EntityTitleContainer = styled.div``;

const IconContainer = styled.div`
    display: flex;
    align-items: flex-start;
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
    matchedFields?: MatchedField[];
}

export default function AutoCompleteEntityItem({
    entity,
    query,
    siblings,
    matchedFields,
}: EntityAutocompleteItemProps) {
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
                        <MatchText
                            color={NAME_COLOR}
                            colorLevel={NAME_COLOR_LEVEL}
                            text={displayName}
                            highlight={query ?? ''}
                        />
                    </EntityTitleContainer>

                    <EntitySubtitle entity={entity} />

                    <Matches matchedFields={matchedFields} entity={entity} query={query} displayName={displayName} />
                </DescriptionContainer>
            </ContentContainer>

            <TypeContainer>
                <Text color={TYPE_COLOR} colorLevel={TYPE_COLOR_LEVEL} size="sm">
                    {displayType}
                </Text>
            </TypeContainer>
        </Container>
    );
}
