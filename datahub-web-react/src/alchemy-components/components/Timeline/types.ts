import React from 'react';

export interface BaseItemType {
    key: string;
}

export type TimelineProps<ItemType extends BaseItemType> = {
    items: ItemType[];
    renderContent: (item: ItemType) => React.ReactNode | JSX.Element;
    renderDot?: (item: ItemType) => React.ReactNode;
};
