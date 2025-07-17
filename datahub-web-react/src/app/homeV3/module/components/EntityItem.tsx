import React from 'react';
import { Link } from 'react-router-dom';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';
import styled from 'styled-components';

const StyledLink = styled(Link)`
    width: 100%;
`;

interface Props {
    entity: Entity;
    customDetailsRenderer?: (entity: Entity) => void;
    navigateOnlyOnNameClick?: boolean;
    hideSubtitle?: boolean;
    hideMatches?: boolean;
}

export default function EntityItem({ entity, customDetailsRenderer, navigateOnlyOnNameClick = false, hideSubtitle, hideMatches }: Props) {
    const entityRegistry = useEntityRegistryV2();

    return (
        <>
            {navigateOnlyOnNameClick ? (
                <AutoCompleteEntityItem
                    entity={entity}
                    key={entity.urn}
                    customDetailsRenderer={customDetailsRenderer}
                    hideSubtitle={hideSubtitle}
                    hideMatches={hideMatches}
                    navigateOnlyOnNameClick
                />
            ) : (
                <StyledLink to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
                    <AutoCompleteEntityItem
                        entity={entity}
                        key={entity.urn}
                        hideSubtitle={hideSubtitle}
                        hideMatches={hideMatches}
                        customDetailsRenderer={customDetailsRenderer}
                    />
                </StyledLink>
            )}
        </>
    );
}
