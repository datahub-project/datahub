import React from 'react';
import { EntityType, Tag } from '../../../types.generated';
import { StyledTag } from '../../entity/shared/components/styled/StyledTag';
import { useEntityRegistry } from '../../useEntityRegistry';

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
