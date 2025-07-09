import React from 'react';
import { Link } from 'react-router-dom';

import AutoCompleteEntityItem from '@app/searchV2/autoCompleteV2/AutoCompleteEntityItem';
import { useEntityRegistryV2 } from '@app/useEntityRegistry';

import { Entity } from '@types';

interface Props {
    entity: Entity;
}

export default function EntityItem({ entity }: Props) {
    const entityRegistry = useEntityRegistryV2();

    return (
        <Link to={entityRegistry.getEntityUrl(entity.type, entity.urn)}>
            <AutoCompleteEntityItem entity={entity} key={entity.urn} />
        </Link>
    );
}
