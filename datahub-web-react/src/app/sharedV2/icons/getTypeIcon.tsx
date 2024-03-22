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
    // TODO: Remove type ignore once EntityRegistry V1 icon method has optional arguments
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    const icon = (subtype && getSubTypeIcon(subtype)) || entityRegistry.getIcon(type, '1em');
    if (includeTitle) {
        return (
            <TitledImage title={capitalizeFirstLetterOnly(subtype) || entityRegistry.getEntityName(type)}>
                {icon}
            </TitledImage>
        );
    }
    return icon;
}
