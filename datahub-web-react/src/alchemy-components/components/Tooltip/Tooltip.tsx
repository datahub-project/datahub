import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';

import colors from '@components/theme/foundations/colors';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    return <Tooltip showArrow={false} color="white" overlayInnerStyle={{ color: colors.gray[1700] }} {...props} />;
}
