import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';
import { useTheme } from 'styled-components';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    const themeConfig = useTheme() as any;
    const bgColor = themeConfig?.colors?.bg ?? 'inherit';
    const textColor = themeConfig?.colors?.textSecondary ?? 'inherit';

    return (
        <Tooltip
            showArrow={false}
            color={bgColor}
            overlayInnerStyle={{ color: textColor }}
            overlayStyle={{ borderRadius: '12px' }}
            {...props}
        />
    );
}
