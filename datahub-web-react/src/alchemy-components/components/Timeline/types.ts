import { TimelineContentDetails } from '@src/app/entityV2/shared/tabs/Incident/types';
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
    items: ItemType[] | TimelineContentDetails[];
    renderContent: (item: ItemType) => React.ReactNode | JSX.Element;
    renderDot?: (item: ItemType) => React.ReactNode;
};
