import React from 'react';

import { StyledTag } from '@app/entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '@app/useEntityRegistry';

import { EntityType, Tag } from '@types';

interface Props {
    tag: Tag;
}

export default function AutoCompleteTag({ tag }: Props) {
    const entityRegistry = useEntityRegistry();

    return (
        <StyledTag $colorHash={tag?.urn} $color={tag?.properties?.colorHex}>
            {entityRegistry.getDisplayName(EntityType.Tag, tag)}
        </StyledTag>
    );
}
