import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';
import { EntityType } from '../../../types.generated';
import { getSubTypeIcon } from '../../entityV2/shared/components/subtypes';
import { EntityRegistry } from '../../../entityRegistryContext';
import { capitalizeFirstLetterOnly } from '../../shared/textUtil';

const StyledTooltip = styled(Tooltip)`
    color: ${REDESIGN_COLORS.TEXT_GREY};
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
