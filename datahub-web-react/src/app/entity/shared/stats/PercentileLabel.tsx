import { Popover, Typography } from 'antd';
import React from 'react';
import { percentileToLabel } from './statsUtils';

type Props = {
    percentile: number;
    description: React.ReactNode;
};

export const PercentileLabel = ({ percentile, description }: Props) => {
    return (
        <Popover
            overlayStyle={{ maxWidth: 260 }}
            placement="top"
            content={<Typography.Text>{description}</Typography.Text>}
        >
            {percentileToLabel(percentile)}
        </Popover>
    );
};
