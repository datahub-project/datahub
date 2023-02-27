import React from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity, EntityType, Tag } from '../../../types.generated';
import AutoCompleteEntity from './AutoCompleteEntity';
import AutoCompleteTag from './AutoCompleteTag';
import AutoCompleteUser from './AutoCompleteUser';

export const SuggestionContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

interface Props {
    query: string;
    entity: Entity;
}

export default function AutoCompleteItem({ query, entity }: Props) {
    let componentToRender: React.ReactNode = null;

    switch (entity.type) {
        case EntityType.CorpUser:
            componentToRender = <AutoCompleteUser query={query} user={entity as CorpUser} />;
            break;
        case EntityType.Tag:
            componentToRender = <AutoCompleteTag tag={entity as Tag} />;
            break;
        default:
            componentToRender = <AutoCompleteEntity query={query} entity={entity} />;
            break;
    }

    return <SuggestionContainer>{componentToRender}</SuggestionContainer>;
}
