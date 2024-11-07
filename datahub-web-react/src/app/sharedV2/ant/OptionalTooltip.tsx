import { Tooltip } from '@components';
import { TooltipProps } from 'antd/lib/tooltip';
import React from 'react';

interface Props {
    tooltipProps: TooltipProps;
    children: React.ReactNode;
    enabled: boolean;
}

export default function OptionalTooltip({ tooltipProps, children, enabled }: Props) {
    if (!enabled) {
        return <>{children}</>;
    }
    return <Tooltip {...tooltipProps}>{children}</Tooltip>;
}
