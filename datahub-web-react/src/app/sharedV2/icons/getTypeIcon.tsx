import React from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { getSubTypeIcon } from '../../entityV2/shared/components/subtypes';
import { EntityRegistry } from '../../../entityRegistryContext';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';

const TitledImage = styled.span`
    line-height: 0;
`;

export default function getTypeIcon(
    entityRegistry: EntityRegistry,
    type: EntityType,
    subtype?: string,
    includeTitle?: boolean,
) {
    const icon = (subtype && getSubTypeIcon(subtype)) || entityRegistry.getIcon(type);
    if (includeTitle) {
        return (
            <TitledImage title={capitalizeFirstLetterOnly(subtype) || entityRegistry.getEntityName(type)}>
                {icon}
            </TitledImage>
        );
    }
    return icon;
}
