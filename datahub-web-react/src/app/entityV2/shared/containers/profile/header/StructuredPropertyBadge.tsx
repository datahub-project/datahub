import React from 'react';
import styled, { useTheme } from 'styled-components';

import { filterForAssetBadge } from '@app/entityV2/shared/containers/profile/header/utils';
import { mapStructuredPropertyToPropertyRow } from '@app/entityV2/shared/tabs/Properties/useStructuredProperties';
import HoverCardAttributionDetails from '@app/sharedV2/propagation/HoverCardAttributionDetails';
import { Pill, Text, Tooltip } from '@src/alchemy-components';
import { getStructuredPropertyValue } from '@src/app/entity/shared/utils';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import { StructuredProperties } from '@src/types.generated';

export const MAX_PROP_BADGE_WIDTH = 150;

const StyledTooltip = styled(Tooltip)`
    .ant-tooltip-inner {
        border-radius: 8px;
        box-shadow: ${(props) => props.theme.colors.shadowSm};
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
    display: flex;
`;

interface Props {
    structuredProperties?: StructuredProperties | null;
}

const StructuredPropertyBadge = ({ structuredProperties }: Props) => {
    const theme = useTheme();
    const badgeStructuredProperty = structuredProperties?.properties?.find(filterForAssetBadge);

    const propRow = badgeStructuredProperty ? mapStructuredPropertyToPropertyRow(badgeStructuredProperty) : undefined;

    if (!badgeStructuredProperty) return null;

    const attribution = propRow?.attribution;
    const propertyValue = propRow?.values[0]?.value;
    const relatedDescription = propRow?.structuredProperty?.definition?.allowedValues?.find(
        (v) => getStructuredPropertyValue(v.value) === propertyValue,
    )?.description;

    const BadgeTooltip = () => {
        return (
            <TooltipContainer>
                <Text weight="semiBold" style={{ color: theme.colors.textSecondary }}>
                    {getDisplayName(badgeStructuredProperty.structuredProperty)}
                </Text>
                <ValueContainer>
                    <Text size="sm" weight="bold" style={{ color: theme.colors.textSecondary }}>
                        Value
                    </Text>
                    <Text style={{ color: theme.colors.textSecondary }}>{propertyValue}</Text>
                </ValueContainer>
                {relatedDescription && (
                    <ValueContainer>
                        <Text size="sm" weight="bold" style={{ color: theme.colors.textSecondary }}>
                            Description
                        </Text>
                        <Text style={{ color: theme.colors.textSecondary }}>{relatedDescription}</Text>
                    </ValueContainer>
                )}
                {attribution && <HoverCardAttributionDetails propagationDetails={{ attribution }} />}
            </TooltipContainer>
        );
    };

    return (
        <StyledTooltip showArrow={false} title={<BadgeTooltip />} overlayInnerStyle={{ width: 250, padding: 16 }}>
            <BadgeContainer>
                <Pill label={propRow?.values[0]?.value?.toString() || ''} size="sm" color="primary" clickable={false} />
            </BadgeContainer>
        </StyledTooltip>
    );
};

export default StructuredPropertyBadge;
