import { colors, Pill, Text, Tooltip } from '@src/alchemy-components';
import { GenericEntityProperties } from '@src/app/entity/shared/types';
import { getDisplayName } from '@src/app/govern/structuredProperties/utils';
import React from 'react';
import styled from 'styled-components';
import { getStructuredPropertyValue } from '@src/app/entity/shared/utils';
import { mapStructuredPropertyToPropertyRow } from '../../../tabs/Properties/useStructuredProperties';

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

interface Props {
    entityData?: GenericEntityProperties | null;
}

const StructuredPropertyBadge = ({ entityData }: Props) => {
    const badgeStructuredProperty = entityData?.structuredProperties?.properties?.filter(
        (prop) => prop.structuredProperty.settings?.showAsAssetBadge && !prop.structuredProperty.settings?.isHidden,
    )[0];

    const propRow = badgeStructuredProperty ? mapStructuredPropertyToPropertyRow(badgeStructuredProperty) : undefined;

    if (!badgeStructuredProperty) return null;

    const propertyValue = propRow?.values[0].value;
    const relatedDescription = propRow?.structuredProperty.definition.allowedValues?.find(
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
                    <Text color="gray">{propRow?.values[0].value}</Text>
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
            <>
                <Pill
                    label={propRow?.values[0].value?.toString() || ''}
                    size="sm"
                    colorScheme="violet"
                    clickable={false}
                />
            </>
        </StyledTooltip>
    );
};

export default StructuredPropertyBadge;
