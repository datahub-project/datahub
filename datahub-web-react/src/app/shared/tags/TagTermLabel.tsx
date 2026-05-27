import React from 'react';

import GlossaryTermPill from '@app/glossaryV2/GlossaryTermPill';
import { getGlossaryTermColor, useGenerateGlossaryColorFromPalette } from '@app/glossaryV2/colorUtils';
import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { Entity, EntityType, GlossaryTerm, Tag } from '@types';

type Props = {
    // default behavior is to accept an entity and render label based on that
    entity?: Entity | null;

    // if no entity is available, for terms just a name may be provided
    termName?: string;
};

export const TagTermLabel = ({ entity, termName }: Props) => {
    const entityRegistry = useEntityRegistry();
    const generateColor = useGenerateGlossaryColorFromPalette();

    if (entity?.type === EntityType.Tag) {
        return (
            <TagPill
                name={entityRegistry.getDisplayName(entity.type, entity)}
                colorHash={(entity as Tag).urn}
                color={(entity as Tag).properties?.colorHex}
                variant="borderless"
            />
        );
    }

    if (entity?.type === EntityType.GlossaryTerm) {
        const term = entity as GlossaryTerm;
        return (
            <GlossaryTermPill
                name={entityRegistry.getDisplayName(entity.type, entity)}
                color={getGlossaryTermColor(term, generateColor)}
                variant="borderless"
            />
        );
    }

    if (termName) {
        return <GlossaryTermPill name={termName} color={generateColor(termName)} variant="borderless" />;
    }
    return null;
};
