/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */
import { Timeline as AntdTimeline } from 'antd';
import React from 'react';

import { StyledAntdTimeline } from '@components/components/Timeline/components';
import { BaseItemType, TimelineProps } from '@components/components/Timeline/types';

export const Timeline = <ItemType extends BaseItemType>({
    items,
    renderDot,
    renderContent,
}: TimelineProps<ItemType>) => {
    return (
        <StyledAntdTimeline>
            {items.map((item) => (
                <AntdTimeline.Item key={item.key} dot={renderDot?.(item)}>
                    {renderContent(item)}
                </AntdTimeline.Item>
            ))}
        </StyledAntdTimeline>
    );
};
