import React from 'react';
import { Link } from 'react-router-dom';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

interface Props {
    entity: Entity;
    customDetailsRenderer?: (entity: Entity) => void;
    navigateOnlyOnNameClick?: boolean;
}

export default function EntityItem({ entity, customDetailsRenderer, navigateOnlyOnNameClick = false }: Props) {
    const entityRegistry = useEntityRegistryV2();

    return (
        <>
            {navigateOnlyOnNameClick ? (
                <AutoCompleteEntityItem
                    entity={entity}
                    key={entity.urn}
                    customDetailsRenderer={customDetailsRenderer}
                    navigateOnlyOnNameClick
                />
            ) : (
                <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
                    <AutoCompleteEntityItem
                        entity={entity}
                        key={entity.urn}
                        customDetailsRenderer={customDetailsRenderer}
                    />
                </Link>
            )}
        </>
    );
}
