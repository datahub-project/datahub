/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip } from '@components';
import React from 'react';
import styled from 'styled-components';

import { getSubTypeIcon } from '@app/entityV2/shared/components/subtypes';
import { REDESIGN_COLORS } from '@app/entityV2/shared/constants';
import { capitalizeFirstLetterOnly } from '@app/shared/textUtil';
import { EntityRegistry } from '@src/entityRegistryContext';

import { EntityType } from '@types';

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
