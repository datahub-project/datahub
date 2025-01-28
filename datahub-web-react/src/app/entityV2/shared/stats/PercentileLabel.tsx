import React from 'react';
import styled from 'styled-components';
import { Popover } from '@components';
import { percentileToColor, percentileToLabel } from './statsUtils';
import { ANTD_GRAY } from '../constants';

const Description = styled.div`
    color: white;
    font-size: 14px;
`;

const Label = styled.span<{ color?: string }>`
    border-radius: 8px;
    padding: 2px 6px;
    background-color: ${(props) => props.color || ANTD_GRAY[3]};
    :hover {
        opacity: 0.7;
    }
    margin-left: 4px;
    color: ${ANTD_GRAY[8]};
`;

type Props = {
    percentile: number;
    description: React.ReactNode;
};

export const PercentileLabel = ({ percentile, description }: Props) => {
    return (
        <Popover
            color="#262626"
            overlayStyle={{ maxWidth: 260 }}
            placement="bottom"
            showArrow={false}
            content={<Description>{description}</Description>}
        >
            <Label color={percentileToColor(percentile)}>{percentileToLabel(percentile)}</Label>
        </Popover>
    );
};
