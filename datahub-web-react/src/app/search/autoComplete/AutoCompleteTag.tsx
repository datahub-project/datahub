/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
