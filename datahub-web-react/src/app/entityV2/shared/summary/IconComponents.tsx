import React from 'react';
import styled from 'styled-components';

import { TYPE_ICON_CLASS_NAME } from '@app/entityV2/shared/components/subtypes';
import { LINEAGE_COLORS } from '@app/entityV2/shared/constants';
import getTypeIcon from '@app/sharedV2/icons/getTypeIcon';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType } from '@types';

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
