import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';

import colors from '@components/theme/foundations/colors';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
<<<<<<< HEAD
    return <Tooltip color="white" overlayInnerStyle={{ color: colors.gray[1700] }} {...props} showArrow={false} />;
||||||| 4e7bb3998d
    return <Tooltip {...props} showArrow={false} />;
=======
    return <Tooltip showArrow={false} color="white" overlayInnerStyle={{ color: colors.gray[1700] }} {...props} />;
>>>>>>> master
}
