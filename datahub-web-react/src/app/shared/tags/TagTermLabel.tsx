import React from 'react';

import TagLabel from '@app/shared/TagLabel';
import TermLabel from '@app/shared/TermLabel';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, Tag } from '@types';

type Props = {
    // default behavior is to accept an entity and render label based on that
    entity?: Entity | null;

    // if no entity is available, for terms just a name may be provided
    termName?: string;
};

export const TagTermLabel = ({ entity, termName }: Props) => {
    const entityRegistry = useEntityRegistry();

    if (entity?.type === EntityType.Tag) {
        return (
            <TagLabel
                name={entityRegistry.getDisplayName(entity.type, entity)}
                colorHash={(entity as Tag).urn}
                color={(entity as Tag).properties?.colorHex}
            />
        );
    }

    if (entity?.type === EntityType.GlossaryTerm) {
        return <TermLabel name={entityRegistry.getDisplayName(entity.type, entity)} />;
    }

    if (termName) {
        return <TermLabel name={termName} />;
    }
    return null;
};
