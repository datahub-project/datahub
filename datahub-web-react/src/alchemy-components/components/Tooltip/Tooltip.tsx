/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Tooltip, TooltipProps } from 'antd';
import * as React from 'react';

import colors from '@components/theme/foundations/colors';

export default function DataHubTooltip(props: TooltipProps & React.RefAttributes<unknown>) {
    return (
        <Tooltip
            showArrow={false}
            color="white"
            overlayInnerStyle={{ color: colors.gray[1700] }}
            overlayStyle={{ borderRadius: '12px' }}
            {...props}
        />
    );
}
