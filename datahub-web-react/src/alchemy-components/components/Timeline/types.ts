import React from 'react';

export type TimelineItem = {
    key: string;
    content: React.ReactNode;
    dot?: React.ReactNode;
};

export interface BaseItemType {
    key: string;
}

export type TimelineProps<ItemType extends BaseItemType> = {
    items: ItemType[];
    renderContent: (item: ItemType) => React.ReactNode;
    renderDot?: (item: ItemType) => React.ReactNode;
};
