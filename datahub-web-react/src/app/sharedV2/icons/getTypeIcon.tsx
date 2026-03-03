import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { IconStyleType } from '@app/entityV2/Entity';
import { getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType } from '@types';

const StyledTooltip = styled(Tooltip)`
    color: currentColor;
`;

const DEFAULT_ICON_SIZE = 14;

export default function getTypeIcon(
    entityRegistry: EntityRegistry,
    type: EntityType,
    subtype?: string,
    includeTitle?: boolean,
    size?: number,
) {
    const iconSize = size || DEFAULT_ICON_SIZE;
    const icon =
        (subtype && getSubTypeIcon(subtype, iconSize)) || entityRegistry.getIcon(type, iconSize, IconStyleType.ACCENT);
    if (includeTitle) {
        return (
            <StyledTooltip
                title={capitalizeFirstLetterOnly(subtype) || entityRegistry.getEntityName(type)}
                mouseEnterDelay={0.3}
                showArrow={false}
            >
                {icon}
            </StyledTooltip>
        );
    }
    return icon;
}
