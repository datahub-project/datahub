import { Tooltip, colors } from '@components';
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
    return (
        <Tooltip {...props} color={colors.white} overlayInnerStyle={{ color: colors.gray[600], borderRadius: 8 }}>
            {children}
        </Tooltip>
    );
}
