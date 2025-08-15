import { MatchText, Popover, zIndices } from '@components';
import React from 'react';
import styled from 'styled-components';

import { FontColorLevelOptions, FontColorOptions, FontSizeOptions, FontWeightOptions } from '@components/theme/config';
import { getColor } from '@components/theme/utils';

import useMeasureIfTrancated from '@app/shared/useMeasureIfTruncated';

const EntityTitleContainer = styled.div<{ $color?: FontColorOptions; $colorLevel?: FontColorLevelOptions }>`
    text-overflow: ellipsis;
    overflow: hidden;
    max-width: 400px;
    color: ${(props) => getColor(props.$color, props.$colorLevel, props.theme)};
`;

const PopoverWrapper = styled.div`
    max-width: 500px;
    overflow-wrap: break-word;
`;

interface Props {
    displayName: string;
    highlight?: string;
    color?: FontColorOptions;
    colorLevel?: FontColorLevelOptions;
    weight?: FontWeightOptions;
    fontSize?: FontSizeOptions;
    className?: string;
    showNameTooltipIfTruncated?: boolean;
}

export default function DisplayName({
    displayName,
    highlight,
    color,
    colorLevel,
    weight,
    fontSize,
    className,
    showNameTooltipIfTruncated,
}: Props) {
    const { measuredRef, isHorizontallyTruncated } = useMeasureIfTrancated();

    return (
        <Popover
            zIndex={zIndices.popover}
            content={
                showNameTooltipIfTruncated && isHorizontallyTruncated ? (
                    <PopoverWrapper>{displayName}</PopoverWrapper>
                ) : undefined
            }
        >
            <EntityTitleContainer ref={measuredRef} className={className} $color={color} $colorLevel={colorLevel}>
                <MatchText
                    type="span"
                    color={color}
                    colorLevel={colorLevel}
                    weight={weight}
                    text={displayName}
                    highlight={highlight ?? ''}
                    size={fontSize}
                />
            </EntityTitleContainer>
        </Popover>
    );
}
