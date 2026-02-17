import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';
import { useTheme } from 'styled-components';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    const themeConfig = useTheme() as any;
    const bgColor = themeConfig?.colors?.bg ?? 'white';
    const textColor = themeConfig?.colors?.textSecondary ?? '#374066';

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
