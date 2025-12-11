/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
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
