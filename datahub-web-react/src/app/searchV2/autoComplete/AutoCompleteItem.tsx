import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components/macro';
import { CorpUser, Entity, EntityType, Tag } from '../../../types.generated';
import { useEntityRegistry } from '../../useEntityRegistry';
import AutoCompleteEntity from './AutoCompleteEntity';
import AutoCompleteTag from './AutoCompleteTag';
import AutoCompleteTooltipContent from './AutoCompleteTooltipContent';
import AutoCompleteUser from './AutoCompleteUser';
import { getShouldDisplayTooltip } from './utils';

export const SuggestionContainer = styled.div`
    display: flex;
    justify-content: left;
    align-items: center;
`;

interface Props {
    query: string;
    entity: Entity;
    siblings?: Array<Entity>;
}

export default function AutoCompleteItem({ query, entity, siblings }: Props) {
    const entityRegistry = useEntityRegistry();
    const displayTooltip = getShouldDisplayTooltip(entity, entityRegistry);
    let componentToRender: React.ReactNode = null;

    switch (entity.type) {
        case EntityType.CorpUser:
            componentToRender = <AutoCompleteUser query={query} user={entity as CorpUser} />;
            break;
        case EntityType.Tag:
            componentToRender = <AutoCompleteTag tag={entity as Tag} />;
            break;
        default:
            componentToRender = (
                <AutoCompleteEntity
                    query={query}
                    entity={entity}
                    siblings={siblings}
                    hasParentTooltip={displayTooltip}
                />
            );
            break;
    }

    return (
        <Tooltip
            overlayStyle={{ maxWidth: 750, visibility: displayTooltip ? 'visible' : 'hidden' }}
            style={{ width: '100%' }}
            title={<AutoCompleteTooltipContent entity={entity} />}
            placement="top"
            color="rgba(0, 0, 0, 0.9)"
        >
            <SuggestionContainer data-testid="auto-complete-option">{componentToRender}</SuggestionContainer>
        </Tooltip>
    );
}
