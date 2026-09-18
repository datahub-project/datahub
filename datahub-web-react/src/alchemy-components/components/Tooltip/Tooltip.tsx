import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';
import { useTheme } from 'styled-components';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    const themeConfig = useTheme();
    const bgColor = themeConfig.colors.bgOverlay;
    const textColor = themeConfig.colors.text;

    return (
        <Tooltip
            showArrow={false}
            color={bgColor}
            overlayInnerStyle={{
                color: textColor,
                border: `1px solid ${themeConfig.colors.border}`,
            }}
            overlayStyle={{ borderRadius: '12px' }}
            {...props}
        />
    );
}
