import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';
import { useTheme } from 'styled-components';

import colors from '@components/theme/foundations/colors';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    const themeConfig = useTheme() as any;
    const bgColor = themeConfig?.colors?.bg ?? 'white';
    const textColor = themeConfig?.colors?.textSecondary ?? colors.gray[1700];

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
