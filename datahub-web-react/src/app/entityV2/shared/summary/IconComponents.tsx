import React from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../../types.generated';
import { TYPE_ICON_CLASS_NAME } from '../components/subtypes';
import getTypeIcon from '../../../sharedV2/icons/getTypeIcon';
import { LINEAGE_COLORS } from '../constants';
import { EntityRegistry } from '../../../../entityRegistryContext';

const IconWrapper = styled.span`
    line-height: 0;
    .${TYPE_ICON_CLASS_NAME} {
        color: ${LINEAGE_COLORS.PURPLE_2};
    }
`;

export function getContentTypeIcon(entityRegistry: EntityRegistry, type: EntityType, subtype?: string) {
    const icon = getTypeIcon(entityRegistry, type, subtype);
    return <IconWrapper>{icon}</IconWrapper>;
}
