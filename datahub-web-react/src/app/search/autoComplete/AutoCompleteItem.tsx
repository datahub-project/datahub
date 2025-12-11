/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from 'antd';
import React from 'react';
import styled from 'styled-components/macro';

import AutoCompleteEntity from '@app/search/autoComplete/AutoCompleteEntity';
import AutoCompleteTag from '@app/search/autoComplete/AutoCompleteTag';
import AutoCompleteTooltipContent from '@app/search/autoComplete/AutoCompleteTooltipContent';
import AutoCompleteUser from '@app/search/autoComplete/AutoCompleteUser';
import { getShouldDisplayTooltip } from '@app/search/autoComplete/utils';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { CorpUser, Entity, EntityType, Tag } from '@types';

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
