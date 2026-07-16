import React from 'react';

import TagPill from '@app/sharedV2/tags/TagPill';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Tag } from '@types';

interface Props {
    tag: Tag;
}

export default function AutoCompleteTag({ tag }: Props) {
    const entityRegistry = useEntityRegistry();
    return (
        <TagPill
            name={entityRegistry.getDisplayName(EntityType.Tag, tag)}
            color={tag?.properties?.colorHex}
            colorHash={tag?.urn}
        />
    );
}
