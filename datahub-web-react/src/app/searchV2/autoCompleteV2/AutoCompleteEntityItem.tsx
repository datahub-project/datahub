import React from 'react';
import styled from 'styled-components';

import DisplayName from '@app/searchV2/autoCompleteV2/components/DisplayName';
import EntityIcon from '@app/searchV2/autoCompleteV2/components/icon/EntityIcon';
import Matches from '@app/searchV2/autoCompleteV2/components/matches/Matches';
import EntitySubtitle from '@app/searchV2/autoCompleteV2/components/subtitle/EntitySubtitle';
import { VARIANT_STYLES } from '@app/searchV2/autoCompleteV2/constants';
import { EntityItemVariant } from '@app/searchV2/autoCompleteV2/types';
import { getEntityDisplayType } from '@app/searchV2/autoCompleteV2/utils';
import { Text } from '@src/alchemy-components';
import { useEntityRegistryV2 } from '@src/app/useEntityRegistry';
import { Entity, MatchedField } from '@src/types.generated';

const Container = styled.div`
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    padding: 8px 13px 8px 8px;
`;

const ContentContainer = styled.div`
    display: flex;
    flex-direction: row;
    gap: 16px;
    overflow: hidden;
    width: 100%;
`;

const DescriptionContainer = styled.div`
    display: flex;
    flex-direction: column;
    overflow: hidden;
    width: 100%;
`;

const IconContainer = styled.div<{ $variant?: EntityItemVariant }>`
    display: flex;
    align-items: ${(props) => {
        switch (props.$variant) {
            case 'searchBar':
                return 'flex-start';
            default:
                return 'center';
        }
    }};
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
    variant?: EntityItemVariant;
}

export default function AutoCompleteEntityItem({
    entity,
    query,
    siblings,
    matchedFields,
    variant,
}: EntityAutocompleteItemProps) {
    const entityRegistry = useEntityRegistryV2();
    const displayName = entityRegistry.getDisplayName(entity.type, entity);
    const displayType = getEntityDisplayType(entity, entityRegistry);
    const variantStyles = VARIANT_STYLES.get(variant ?? 'default');

    return (
        <Container>
            <ContentContainer>
                <IconContainer $variant={variant}>
                    <EntityIcon entity={entity} siblings={siblings} />
                </IconContainer>

                <DescriptionContainer>
                    <DisplayName displayName={displayName} highlight={query} />

                    <EntitySubtitle entity={entity} />

                    <Matches matchedFields={matchedFields} entity={entity} query={query} displayName={displayName} />
                </DescriptionContainer>
            </ContentContainer>

            <TypeContainer>
                <Text color={variantStyles?.typeColor} colorLevel={variantStyles?.typeColorLevel} size="sm">
                    {displayType}
                </Text>
            </TypeContainer>
        </Container>
    );
}
