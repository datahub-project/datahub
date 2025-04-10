import { Timeline as AntdTimeline } from 'antd';
import React from 'react';
import { StyledAntdTimeline } from './components';
import { BaseItemType, TimelineProps } from './types';

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
