import { Popover } from '@components';
import React from 'react';
import styled, { useTheme } from 'styled-components';

import { percentileToColor, percentileToLabel } from '@app/entityV2/shared/stats/statsUtils';

const Description = styled.div`
    color: ${(props) => props.theme.colors.textOnFillBrand};
    font-size: 14px;
`;

const Label = styled.span<{ color?: string }>`
    border-radius: 8px;
    padding: 2px 6px;
    background-color: ${(props) => props.color || props.theme.colors.bgSurface};
    :hover {
        opacity: 0.7;
    }
    margin-left: 4px;
    color: ${(props) => props.theme.colors.textSecondary};
`;

type Props = {
    percentile: number;
    description: React.ReactNode;
};

export const PercentileLabel = ({ percentile, description }: Props) => {
    const theme = useTheme();
    return (
        <Popover
            color={theme.colors.bgTooltip}
            overlayStyle={{ maxWidth: 260 }}
            placement="bottom"
            showArrow={false}
            content={<Description>{description}</Description>}
        >
            <Label color={percentileToColor(percentile)}>{percentileToLabel(percentile)}</Label>
        </Popover>
    );
};
