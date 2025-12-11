/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import React from 'react';
import styled from 'styled-components';

import { filterForAssetBadge } from '@app/entity/shared/containers/profile/header/utils';
import { mapStructuredPropertyToPropertyRow } from '@app/entity/shared/tabs/Properties/useStructuredProperties';
import { Pill, Text, Tooltip, colors } from '@src/alchemy-components';
import { getStructuredPropertyValue } from '@src/app/entity/shared/utils';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { StructuredProperties } from '@src/types.generated';

export const MAX_PROP_BADGE_WIDTH = 150;

const StyledTooltip = styled(Tooltip)`
    .ant-tooltip-inner {
        border-radius: 8px;
        box-shadow: 0px 4px 12px 0px rgba(9, 1, 61, 0.12);
    }
`;

const TooltipContainer = styled.div`
    display: flex;
    flex-direction: column;
    gap: 16px;
`;

const ValueContainer = styled.div`
    gap: 4px;
`;

const BadgeContainer = styled.div`
    max-width: ${MAX_PROP_BADGE_WIDTH}px;
`;

interface Props {
    structuredProperties?: StructuredProperties | null;
}

const StructuredPropertyBadge = ({ structuredProperties }: Props) => {
    const badgeStructuredProperty = structuredProperties?.properties?.find(filterForAssetBadge);

    const propRow = badgeStructuredProperty ? mapStructuredPropertyToPropertyRow(badgeStructuredProperty) : undefined;

    if (!badgeStructuredProperty) return null;

    const propertyValue = propRow?.values[0]?.value;
    const relatedDescription = propRow?.structuredProperty?.definition?.allowedValues?.find(
        (v) => getStructuredPropertyValue(v.value) === propertyValue,
    )?.description;

    const BadgeTooltip = () => {
        return (
            <TooltipContainer>
                <Text color="gray" weight="semiBold">
                    {getDisplayName(badgeStructuredProperty.structuredProperty)}
                </Text>
                <ValueContainer>
                    <Text color="gray" size="sm" weight="bold">
                        Value
                    </Text>
                    <Text color="gray">{propRow?.values[0]?.value}</Text>
                </ValueContainer>
                {relatedDescription && (
                    <ValueContainer>
                        <Text color="gray" size="sm" weight="bold">
                            Description
                        </Text>
                        <Text color="gray">{relatedDescription}</Text>
                    </ValueContainer>
                )}
            </TooltipContainer>
        );
    };

    return (
        <StyledTooltip
            showArrow={false}
            title={<BadgeTooltip />}
            color={colors.white}
            overlayInnerStyle={{ width: 250, padding: 16 }}
        >
            <BadgeContainer>
                <Pill label={propRow?.values[0]?.value?.toString() || ''} size="sm" color="violet" clickable={false} />
            </BadgeContainer>
        </StyledTooltip>
    );
};

export default StructuredPropertyBadge;
