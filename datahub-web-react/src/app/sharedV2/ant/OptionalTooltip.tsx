import { Tooltip } from '@components';
import { TooltipProps } from 'antd/lib/tooltip';
import React from 'react';

type Props = TooltipProps & {
    children: React.ReactNode;
    enabled: boolean;
};

export default function OptionalTooltip({ children, enabled, ...props }: Props) {
    if (!enabled) {
        return <>{children}</>;
    }
    return <Tooltip {...props}>{children}</Tooltip>;
}
